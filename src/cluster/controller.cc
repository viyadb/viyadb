#include <chrono>
#include <glog/logging.h>
#include "cluster/controller.h"
#include "cluster/notifier.h"
#include "cluster/plan.h"

namespace viya {
namespace cluster {

Controller::Controller(const util::Config& config):
  cluster_id_(config.str("cluster_id")),
  consul_(config),
  batch_id_(0L) {

  ReadClusterConfig();
  ReadTablesConfigs();
  ReadIndexersConfigs();

  session_ = consul_.CreateSession(std::string("viyadb-controller"));
  le_ = consul_.ElectLeader(*session_, "clusters/" + cluster_id_ + "/nodes/controller/leader");

  plan_initializer_ = std::make_unique<util::Later>(10000L, [this]() {
    InitializePlan();
  });
}

void Controller::ReadClusterConfig() {
  cluster_config_ = util::Config(consul_.GetKey("clusters/" + cluster_id_ + "/config"));
  LOG(INFO)<<"Using cluster configuration: "<<cluster_config_.dump();
}

void Controller::ReadTablesConfigs() {
  LOG(INFO)<<"Reading tables configurations";
  tables_configs_.clear();
  for (auto& table : cluster_config_.strlist("tables", {})) {
    tables_configs_[table] = util::Config(consul_.GetKey("tables/" + table + "/config"));
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

void Controller::ReadIndexersConfigs() {
  LOG(INFO)<<"Reading indexers configurations";
  indexers_configs_.clear();
  for (auto& indexer_id : cluster_config_.strlist("indexers", {})) {
    indexers_configs_.emplace(indexer_id, consul_.GetKey("indexers/" + indexer_id + "/config"));
  }
}

void Controller::FetchLatestBatchInfo() {
  LOG(INFO)<<"Fetching latest batch info from indexers";
  for (auto& conf_it : indexers_configs_) {

    auto& indexer_conf = conf_it.second;
    auto notifier = NotifierFactory::Create(indexer_conf.sub("batch").sub("notifier"));
    auto info = notifier->GetLastMessage();

    if (!info.empty()) {
      uint64_t id = info["id"];
      if (batch_id_ == 0L) {
        batch_id_ = id;
      } else if (id != batch_id_) {
        throw std::runtime_error(
          "Batch indexers are not aligned! Different batch ID were read from notification channels");
      }

      json tables_info = info["tables"];
      tables_partitions_.clear();
      for (auto info_it = tables_info.begin(); info_it != tables_info.end(); ++info_it) {
        std::string table_name = info_it.key();
        if (tables_configs_.find(table_name) != tables_configs_.end()) {
          tables_partitions_.emplace(table_name, info_it.value());
        }
      }
    }
  }
}

void Controller::InitializePlan() {
  FetchLatestBatchInfo();
  if (batch_id_ == 0L) {
    LOG(WARNING)<<"Latest batch info is not available";
  } else {
    while (true) {
      if (le_->Leader()) {
        GeneratePlan();
        break;
      }
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
  if (!existing_plan.empty() && existing_plan["batch_id"].get<uint64_t>() == batch_id_) {

    LOG(INFO)<<"Reading cached plan from Consul";
    tables_plans_.clear();

    json tables_plans = existing_plan["plan"];
    for (auto it = tables_plans.begin(); it != tables_plans.end(); ++it) {
      tables_plans_.emplace(it.key(), it.value());
    }
    return true;
  }
  return false;
}

void Controller::GeneratePlan() {
  while (!ReadWorkersConfigs()) {
    std::this_thread::sleep_for(std::chrono::seconds(10));
  }

  LOG(INFO)<<"Generating partitioning plan";
  PlanGenerator plan_generator(cluster_config_);
  tables_plans_.clear();

  for (auto& it : tables_partitions_) {
    auto& table_name = it.first;
    auto& partitions = it.second;
    tables_plans_.emplace(
        table_name, std::move(plan_generator.Generate(partitions.partitions_num(), workers_configs_)));
  }

  LOG(INFO)<<"Storing partitioning plan into Consul";
  json plans_cache = json({});
  for (auto& it : tables_plans_) {
    plans_cache[it.first] = it.second.ToJson();
  }
  json cache = {
    {"plan", plans_cache},
    {"batch_id", batch_id_}
  };
  consul_.PutKey("clusters/" + cluster_id_ + "/plan", cache.dump());
}

}}
