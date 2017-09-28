#include <sstream>
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

    repeat_ = std::make_unique<util::Repeat>(5000, [this]() {
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
  PlanGenerator plan_generator(cluster_config_);
  json plan = json({});

  LOG(INFO)<<"Finding active workers";
  std::vector<util::Config> worker_configs;
  for (auto key : consul_.ListKeys(cluster_id_ + "/nodes/workers")) {
    worker_configs.emplace(worker_configs.end(), consul_.GetKey(cluster_id_ + "/nodes/workers/" + key, "{}"));
  }

  for (auto& table_conf : table_configs_) {
    auto table = table_conf.first;

    LOG(INFO)<<"Retreiving partitioning for table: "<<table;
    auto partitions = json::parse(consul_.GetKey(
        "tables/" + table + "/partitions/" + cluster_config_.str("batch"))).get<json>();

    plan[table] = plan_generator.Generate(partitions, worker_configs);
  }

  DLOG(INFO)<<"Generated plan: "<<plan.dump(2);
}

}}
