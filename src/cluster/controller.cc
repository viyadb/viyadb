/*
 * Copyright (c) 2017-present ViyaDB Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cluster/controller.h"
#include "cluster/configurator.h"
#include "cluster/notifier.h"
#include "util/hostname.h"
#include <boost/exception/diagnostic_information.hpp>
#include <chrono>
#include <glog/logging.h>

namespace viya {
namespace cluster {

Controller::Controller(const util::Config &config)
    : id_(util::get_hostname()), config_(config),
      cluster_id_(config.str("cluster_id")), consul_(config),
      db_(config, 0, 0) {

  ReadClusterConfig();

  session_ = consul_.CreateSession(std::string("viyadb-controller"));
  le_ = consul_.ElectLeader(*session_,
                            "clusters/" + cluster_id_ + "/nodes/leader");

  workers_watch_ = std::make_unique<WorkersWatch>(*this);

  initializer_ = std::make_unique<util::Later>(100L, [this]() {
    try {
      Initialize();
    } catch (...) {
      LOG(ERROR) << "Error initializing controller: "
                 << boost::current_exception_diagnostic_information();
    }
  });
}

void Controller::ReadClusterConfig() {
  cluster_config_ =
      util::Config(consul_.GetKey("clusters/" + cluster_id_ + "/config"));

  // Set defaults:
  if (!cluster_config_.exists("replication_factor")) {
    cluster_config_.set_num("replication_factor", 3);
  }
  if (!cluster_config_.exists("http_port")) {
    cluster_config_.set_num("http_port", 5555);
  }

  LOG(INFO) << "Using cluster configuration: " << cluster_config_.dump();

  indexers_configs_.clear();
  for (auto &indexer_id : cluster_config_.strlist("indexers", {})) {
    indexers_configs_.emplace(
        indexer_id, consul_.GetKey("indexers/" + indexer_id + "/config"));
  }
  LOG(INFO) << "Read " << indexers_configs_.size()
            << " indexers configurations";

  tables_configs_.clear();
  for (auto &table : cluster_config_.strlist("tables", {})) {
    auto &table_conf =
        tables_configs_
            .emplace(table, consul_.GetKey("tables/" + table + "/config"))
            .first->second;

    if (!FindIndexerForTable(table).empty()) {
      // Adapt metrics for indexers output:
      json *raw_config = reinterpret_cast<json *>(table_conf.json_ptr());
      for (auto &metric : (*raw_config)["metrics"]) {
        if (metric["type"] == "count") {
          metric["type"] = "long_sum";
        }
      }
    }

    db_.CreateTable(table_conf);
  }
  LOG(INFO) << "Read " << tables_configs_.size() << " tables configurations";
}

bool Controller::IsOwnWorker(const std::string &worker_id) const {
  return worker_id.substr(0, worker_id.find(":")) == id_;
}

void Controller::Initialize() {
  InitializePlan();

  std::string load_prefix = config_.str("state_dir") + "/input";

  Configurator configurator(*this);
  configurator.ConfigureWorkers();

  feeder_ = std::make_unique<Feeder>(*this, load_prefix);

  CreateKey();

  StartHttpServer();
}

void Controller::FetchLatestBatchInfo() {
  indexers_batches_.clear();
  for (auto &it : indexers_configs_) {
    auto notifier = NotifierFactory::Create(
        it.first, it.second.sub("batch").sub("notifier"), IndexerType::BATCH);
    auto msg = notifier->GetLastMessage();
    if (msg) {
      indexers_batches_.emplace(
          it.first, std::move(static_cast<BatchInfo *>(msg.release())));
    }
  }
  LOG(INFO) << "Fetched " << indexers_batches_.size()
            << " batches from indexers notifiers";
}

std::string
Controller::FindIndexerForTable(const std::string &table_name) const {
  for (auto &indexer_it : indexers_configs_) {
    auto indexer_tables = indexer_it.second.strlist("tables");
    if (std::find(indexer_tables.begin(), indexer_tables.end(), table_name) !=
        indexer_tables.end()) {
      return indexer_it.first;
    }
  }
  return std::string();
}

void Controller::InitializePartitioning(size_t replication_factor) {
  FetchLatestBatchInfo();

  tables_partitioning_.clear();

  if (indexers_batches_.empty()) {
    LOG(WARNING) << "No historical batches information available - generating "
                    "default partitioning";

    for (auto &table_it : tables_configs_) {
      auto &table_name = table_it.first;
      auto &table_conf = table_it.second;

      util::Config partitioning;
      if (table_conf.exists("partitioning")) {
        partitioning = table_conf.sub("partitioning");
      } else {
        // Take partitioning config from indexer responsible for that table:
        auto indexer_id = FindIndexerForTable(table_name);
        if (!indexer_id.empty()) {
          auto batch_conf = indexers_configs_.at(indexer_id).sub("batch");
          if (batch_conf.exists("partitioning")) {
            partitioning = batch_conf.sub("partitioning");
          }
        }
      }

      if (!partitioning.exists("columns")) {
        throw std::runtime_error("Table partitioning must present in either "
                                 "table or indexer configuration!");
      }

      size_t default_partitions_num = total_workers_num() / replication_factor;

      size_t total_partitions =
          partitioning.num("partitions", default_partitions_num);

      // Every key value goes to a partition:
      std::vector<uint32_t> mapping;
      mapping.resize(total_partitions);
      for (uint32_t v = 0; v < total_partitions; ++v) {
        mapping[v] = v;
      }
      tables_partitioning_.emplace(
          std::piecewise_construct, std::forward_as_tuple(table_name),
          std::forward_as_tuple(mapping, total_partitions,
                                partitioning.strlist("columns")));
    }
  } else {
    for (auto &batches_it : indexers_batches_) {
      for (auto &tables_it : batches_it.second->tables_info()) {
        auto &table_name = tables_it.first;
        if (tables_partitioning_.find(table_name) !=
            tables_partitioning_.end()) {
          throw std::runtime_error("Multiple indexers operate on same tables!");
        }
        // TODO: check whether partitioning is available (or make it mandatory
        // on indexer side)
        tables_partitioning_.emplace(table_name,
                                     tables_it.second.partitioning());
      }
    }
  }
}

void Controller::InitializePlan() {
  while (true) {
    if (le_->Leader()) {
      if (GeneratePlan()) {
        break;
      }
      LOG(INFO) << "Can't generate or store partitioning plan right now... "
                   "will retry soon";
      std::this_thread::sleep_for(std::chrono::seconds(10));
    } else {
      if (ReadPlan()) {
        break;
      }
      LOG(INFO) << "Partitioning plan is not available yet... waiting for "
                   "leader to generate it";
      std::this_thread::sleep_for(std::chrono::seconds(10));
    }
  }
}

bool Controller::ReadPlan() {
  json cache = json::parse(
      consul_.GetKey("clusters/" + cluster_id_ + "/plan", false, "{}"));
  if (cache.empty()) {
    return false;
  }

  LOG(INFO) << "Reading cached plan from Consul";

  tables_plans_.clear();
  json tables_plans = cache["plan"];
  for (auto it = tables_plans.begin(); it != tables_plans.end(); ++it) {
    tables_plans_.emplace(it.key(), it.value());
  }

  tables_partitioning_.clear();
  json tables_part = cache["partitioning"];
  for (auto it = tables_part.begin(); it != tables_part.end(); ++it) {
    tables_partitioning_.emplace(it.key(), it.value());
  }
  return true;
}

bool Controller::GeneratePlan() {
  size_t replication_factor = cluster_config_.num("replication_factor");

  LOG(INFO) << "Initializing table partitioning";
  InitializePartitioning(replication_factor);

  LOG(INFO) << "Waiting for all workers to connect";
  workers_watch_->WaitForAllWorkers();

  LOG(INFO) << "Generating partitioning plan";
  PlanGenerator plan_generator;
  tables_plans_.clear();

  auto active_workers = workers_watch_->GetActiveWorkers();
  for (auto &it : tables_partitioning_) {
    tables_plans_.emplace(it.first, plan_generator.Generate(it.second.total(),
                                                            replication_factor,
                                                            active_workers));
  }

  LOG(INFO) << "Storing plan to Consul";
  json cached_plan = json({});
  for (auto &it : tables_plans_) {
    cached_plan[it.first] = it.second.ToJson();
  }
  json cached_partitioning = json({});
  for (auto &it : tables_partitioning_) {
    cached_partitioning[it.first] = it.second.ToJson();
  }
  json cache =
      json({{"plan", cached_plan}, {"partitioning", cached_partitioning}});
  return session_->EphemeralKey("clusters/" + cluster_id_ + "/plan",
                                cache.dump());
}

void Controller::StartHttpServer() {
  http_service_ = std::make_unique<http::Service>(*this);
  http_service_->Start();
}

void Controller::CreateKey() const {
  auto controller_key =
      "clusters/" + config_.str("cluster_id") + "/nodes/controllers/" + id_;

  util::Config controller_data;
  controller_data.set_num("http_port", cluster_config_.num("http_port"));
  controller_data.set_str("hostname", util::get_hostname());
  controller_data.set_str("id", id_);

  while (!session_->EphemeralKey(controller_key, controller_data.dump())) {
    LOG(WARNING) << "The controller key is still locked by the previous "
                    "process... waiting";
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  }
}

} // namespace cluster
} // namespace viya
