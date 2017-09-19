#include "coord/controller.h"

namespace viya {
namespace coord {

Controller::Controller(const util::Config& config)
  :cluster_id_(config.str("cluster_id")) {

  consul_ = std::make_unique<Consul>(config);

  if (consul_->Enabled()) {
    session_ = consul_->CreateSession(std::string("viyadb-controller"));
    le_ = consul_->ElectLeader(*session_, cluster_id_ + "/nodes/controller/leader");
  }
}

}}
