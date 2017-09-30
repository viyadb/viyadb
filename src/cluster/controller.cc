#include <sstream>
#include <set>
#include <json.hpp>
#include <glog/logging.h>
#include "cluster/controller.h"
#include "cluster/plan.h"

namespace viya {
namespace cluster {

using json = nlohmann::json;

Controller::Controller(const util::Config& config)
  :cluster_id_(config.str("cluster_id")),consul_(config) {

  if (consul_.Enabled()) {
    ReadClusterConfig();

    session_ = consul_.CreateSession(std::string("viyadb-controller"));
    le_ = consul_.ElectLeader(*session_, cluster_id_ + "/nodes/controller/leader");

    repeat_ = std::make_unique<util::Repeat>(30000, [this]() {
      if (le_->Leader()) {
        try {
          GeneratePlan();
        } catch (std::exception& e) {
          LOG(ERROR)<<"There was a problem while generating a plan: "<<e.what();
        }
      }
    });
  }
}

void Controller::ReadClusterConfig() {
  cluster_config_ = util::Config(consul_.GetKey(cluster_id_ + "/config"));
  LOG(INFO)<<"Using cluster configuration: "<<cluster_config_.dump();

  LOG(INFO)<<"Reading tables configurations";
  table_configs_.clear();
  for (auto& table : cluster_config_.strlist("tables")) {
    table_configs_[table] = util::Config(
      consul_.GetKey("tables/" + table + "/config"));
  }
}

void Controller::GeneratePlan() {
  DLOG(INFO)<<"Finding active workers";
  auto active_workers = consul_.ListKeys(cluster_id_ + "/nodes/workers");
  if (active_workers != cached_workers_) {

    LOG(INFO)<<"Found new active workers";
    std::vector<util::Config> worker_configs;
    for (auto worker : active_workers) {
      // TODO: Validate worker configuration
      worker_configs.emplace(worker_configs.end(),
                             consul_.GetKey(cluster_id_ + "/nodes/workers/" + worker, "{}"));
    }

    PlanGenerator plan_generator(cluster_config_);
    json plan = json({});

    for (auto& table_conf : table_configs_) {
      auto table = table_conf.first;

      LOG(INFO)<<"Retreiving partitioning for table: "<<table;
      std::ostringstream key;
      key<<"tables/"<<table<<"/batch/"<<cluster_config_.str("batch")<<"/partitions";
      auto partitions = json::parse(consul_.GetKey(key.str())).get<std::map<std::string, int>>();
      std::set<int> dist_parts;
      for (auto& p : partitions) {
        dist_parts.insert(p.second);
      }

      plan[table] = plan_generator.Generate(dist_parts.size(), worker_configs).ToJson();
    }

    consul_.PutKey(cluster_id_ + "/plan", plan.dump());
    cached_workers_ = active_workers;
  }
}

}}
