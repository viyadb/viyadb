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

#ifndef VIYA_CLUSTER_CONSUL_LEADER_H_
#define VIYA_CLUSTER_CONSUL_LEADER_H_

#include "cluster/consul/watch.h"
#include "util/schedule.h"
#include <atomic>
#include <memory>

namespace viya {
namespace cluster {
namespace consul {

class Consul;
class Session;

class LeaderElector {
public:
  LeaderElector(const Consul &consul, const Session &session,
                const std::string &key);
  LeaderElector(const LeaderElector &other) = delete;

  bool Leader() const { return leader_; }

private:
  void Start();

private:
  const Session &session_;
  const std::string key_;
  std::unique_ptr<Watch> watch_;
  std::atomic<bool> leader_;
  std::unique_ptr<util::Always> always_;
};
}
}
}

#endif // VIYA_CLUSTER_CONSUL_LEADER_H_
