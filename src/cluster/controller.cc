#include <glog/logging.h>
#include "cluster/controller.h"

namespace viya {
namespace cluster {

Controller::Controller(const util::Config& config)
  :cluster_id_(config.str("cluster_id")),consul_(config) {

  if (consul_.Enabled()) {
    session_ = consul_.CreateSession(std::string("viyadb-controller"));
    le_ = consul_.ElectLeader(*session_, cluster_id_ + "/nodes/controller/leader");

    repeat_ = std::make_unique<util::Repeat>(60000, [this]() {
      if (le_->Leader()) {
        GeneratePlan();
      }
    });
  }
}

void Controller::GeneratePlan() const {
  LOG(INFO)<<"Looking for connected workers";

  for (auto key : consul_.ListKeys(cluster_id_ + "/nodes/workers")) {
  }
}

}}
