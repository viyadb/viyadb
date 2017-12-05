#include <chrono>
#include <glog/logging.h>
#include "cluster/controller.h"
#include "cluster/notifier.h"

namespace viya {
namespace cluster {

Controller::Controller(const util::Config& config):
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
  batches_.clear();

  for (auto& it : indexers_configs_) {
    auto& indexer_id = it.first;
    auto& indexer_conf = it.second;

    auto notifier = NotifierFactory::Create(indexer_conf.sub("batch").sub("notifier"), IndexerType::BATCH);
    auto info = notifier->GetLastMessage();
    if(!info) {
      continue;
    }
    batches_.emplace(indexer_id, std::move(static_cast<BatchInfo*>(info.release())));
  }
}

void Controller::Initialize() {
  FetchLatestBatchInfo();

  InitializePlan();

  feeder_ = std::make_unique<Feeder>(*this);
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
    tables_plans_.emplace(it.key(), it.value());
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

  for (auto& it : batches_) {
    auto& batch_info = it.second;

    for (auto& pit : batch_info->tables_partitions()) {
      auto& table_name = pit.first;
      if (tables_plans_.find(table_name) != tables_plans_.end()) {
        throw std::runtime_error("Multiple indexers operate on same tables!");
      }
      auto& table_partitions = pit.second;
      tables_plans_.emplace(table_name, std::move(
          plan_generator.Generate(table_partitions.partitions_num(), workers_configs_)));
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
