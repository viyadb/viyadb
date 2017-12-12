/*
 * Copyright (c) 2017 ViyaDB Group
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

#include <chrono>
#include <glog/logging.h>
#include "cluster/controller.h"
#include "cluster/notifier.h"
#include "cluster/configurator.h"

namespace viya {
namespace cluster {

Controller::Controller(const util::Config& config):
  config_(config),
  cluster_id_(config.str("cluster_id")),
  consul_(config) {

  ReadClusterConfig();

  session_ = consul_.CreateSession(std::string("viyadb-controller"));
  le_ = consul_.ElectLeader(*session_, "clusters/" + cluster_id_ + "/nodes/controller/leader");

  initializer_ = std::make_unique<util::Later>(10000L, [this]() {
    Initialize();
  });
}

void Controller::ReadClusterConfig() {
  cluster_config_ = util::Config(consul_.GetKey("clusters/" + cluster_id_ + "/config"));
  LOG(INFO)<<"Using cluster configuration: "<<cluster_config_.dump();

  LOG(INFO)<<"Reading tables configurations";
  tables_configs_.clear();
  for (auto& table : cluster_config_.strlist("tables", {})) {
    tables_configs_[table] = util::Config(consul_.GetKey("tables/" + table + "/config"));
  }

  LOG(INFO)<<"Reading indexers configurations";
  indexers_configs_.clear();
  for (auto& indexer_id : cluster_config_.strlist("indexers", {})) {
    indexers_configs_.emplace(indexer_id, consul_.GetKey("indexers/" + indexer_id + "/config"));
  }
}

bool Controller::ReadWorkersConfigs() {
  DLOG(INFO)<<"Finding active workers";

  auto active_workers = consul_.ListKeys("clusters/" + cluster_id_ + "/nodes/workers");
  auto minimum_workers = (size_t)cluster_config_.num("minimum_workers", 0L);
  if (minimum_workers > 0 && active_workers.size() < minimum_workers) {
    LOG(INFO)<<"Number of active workers is less than the minimal number of workers ("<<minimum_workers<<")";
    return false;
  }

  DLOG(INFO)<<"Reading workers configurations";
  workers_configs_.clear();
  for (auto& worker_id : active_workers) {
    workers_configs_.emplace(worker_id, consul_.GetKey(
        "clusters/" + cluster_id_ + "/nodes/workers/" + worker_id, false, "{}"));
  }
  return true;
}

void Controller::FetchLatestBatchInfo() {
  LOG(INFO)<<"Fetching latest batches info from indexers notifiers";
  indexers_batches_.clear();

  for (auto& it : indexers_configs_) {
    auto& indexer_id = it.first;
    auto& indexer_conf = it.second;

    auto notifier = NotifierFactory::Create(indexer_conf.sub("batch").sub("notifier"), IndexerType::BATCH);
    auto info = notifier->GetLastMessage();
    if (info) {
      indexers_batches_.emplace(indexer_id, std::move(static_cast<BatchInfo*>(info.release())));
    }
  }
}

void Controller::Initialize() {
  FetchLatestBatchInfo();

  InitializePlan();

  std::string load_prefix = config_.str("state_dir") + "/input";

  Configurator configurator(*this, load_prefix);
  configurator.ConfigureWorkers();

  feeder_ = std::make_unique<Feeder>(*this, load_prefix);
}

void Controller::InitializePlan() {
  while (true) {
    if (le_->Leader()) {
      if (GeneratePlan()) {
        break;
      }
      LOG(INFO)<<"Can't generate or store partitioning plan right now... will retry soon";
      std::this_thread::sleep_for(std::chrono::seconds(10));
    } else {
      if (ReadPlan()) {
        break;
      }
      LOG(INFO)<<"Partitioning plan is not available yet... waiting for leader to generate it";
      std::this_thread::sleep_for(std::chrono::seconds(10));
    }
  }
}

bool Controller::ReadPlan() {
  json existing_plan = json::parse(consul_.GetKey("clusters/" + cluster_id_ + "/plan", false, "{}"));
  if (existing_plan.empty()) {
    return false;
  }
  LOG(INFO)<<"Reading cached plan from Consul";
  tables_plans_.clear();
  json tables_plans = existing_plan["plan"];
  for (auto it = tables_plans.begin(); it != tables_plans.end(); ++it) {
    tables_plans_.emplace(std::piecewise_construct,
                          std::forward_as_tuple(it.key()),
                          std::forward_as_tuple(it.value(), workers_configs_));
  }
  return true;
}

bool Controller::GeneratePlan() {
  while (!ReadWorkersConfigs()) {
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }

  LOG(INFO)<<"Generating partitioning plan";
  PlanGenerator plan_generator(cluster_config_);
  tables_plans_.clear();

  for (auto& it : indexers_batches_) {
    for (auto& tit : it.second->tables_info()) {
      auto& table_name = tit.first;
      if (tables_plans_.find(table_name) != tables_plans_.end()) {
        throw std::runtime_error("Multiple indexers operate on same tables!");
      }
      auto& table_info = tit.second;
      tables_plans_.emplace(table_name, std::move(
          plan_generator.Generate(table_info.total_partitions(), workers_configs_)));
    }
  }

  LOG(INFO)<<"Storing partitioning plan to Consul";
  json cache = json({});
  for (auto& it : tables_plans_) {
    cache[it.first] = it.second.ToJson();
  }
  return session_->EphemeralKey("clusters/" + cluster_id_ + "/plan", cache.dump());
}

}}
