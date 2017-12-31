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
#include <stdexcept>
#include <glog/logging.h>
#include "cluster/consul/leader.h"
#include "cluster/consul/session.h"
#include "cluster/consul/consul.h"

namespace viya {
namespace cluster {
namespace consul {

LeaderElector::LeaderElector(const Consul& consul, const Session& session, const std::string& key)
  :session_(session),key_(key),watch_(consul.WatchKey(key)),leader_(false) {

  Start();
}

void LeaderElector::Start() {
  always_ = std::make_unique<util::Always>([this]() {
    if (!leader_) {
      try {
        leader_ = session_.EphemeralKey(key_, std::string(""));
        if (leader_) {
          LOG(INFO)<<"Became a leader";
        }
      } catch (std::exception& e) {
        LOG(WARNING)<<"Can't elect leader ("<<e.what()<<")";
      }
    }
    try {
      auto changes = watch_->LastChanges();
      if (changes && leader_ && session_.id() != (*changes)["Session"]) {
        LOG(INFO)<<"Not a leader anymore";
        leader_ = false;
      }
    } catch (std::exception& e) {
      LOG(WARNING)<<"Error watching leader status ("<<e.what()<<")";
      leader_ = false;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  });
}

}}}
