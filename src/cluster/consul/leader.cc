#include <chrono>
#include <stdexcept>
#include <glog/logging.h>
#include "cluster/consul/leader.h"
#include "cluster/consul/session.h"
#include "cluster/consul/consul.h"

namespace viya {
namespace cluster {

LeaderElector::LeaderElector(const Consul& consul, const Session& session, const std::string& key)
  :session_(session),key_(key),watch_(consul.WatchKey(key)),leader_(false) {

  Start();
}

void LeaderElector::Start() {
  always_ = std::make_unique<util::Always>([this]() {
    try {
      if (!leader_) {
        leader_ = session_.EphemeralKey(key_, std::string(""));
        if (leader_) {
          LOG(INFO)<<"Became leader";
        }
      }
      auto changes = watch_->LastChanges();
      if (leader_ && session_.id() != changes["Session"]) {
        LOG(INFO)<<"Not leader anymore";
        leader_ = false;
      }
    } catch (std::exception& e) {
      LOG(WARNING)<<"Can't elect leader ("<<e.what()<<")";
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  });
}

}}
