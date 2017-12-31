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

#ifndef VIYA_CLUSTER_CONSUL_CONSUL_H_
#define VIYA_CLUSTER_CONSUL_CONSUL_H_

#include <memory>
#include <functional>
#include "util/config.h"
#include "cluster/consul/session.h"
#include "cluster/consul/service.h"
#include "cluster/consul/leader.h"
#include "cluster/consul/watch.h"

namespace viya {
namespace cluster {
namespace consul {

namespace util = viya::util;

class Consul {
  public:
    Consul(const util::Config& config);
    Consul(const Consul& other) = delete;

    std::unique_ptr<Session> CreateSession(const std::string& name, uint32_t ttl_sec = 10) const;

    std::unique_ptr<Service> RegisterService(const std::string& name,
        uint16_t port, uint32_t ttl_sec = 10, bool auto_hc = true) const;

    std::unique_ptr<LeaderElector> ElectLeader(const Session& session, const std::string& key) const;

    std::unique_ptr<Watch> WatchKey(const std::string& key, bool recurse = false) const;
    std::vector<std::string> ListKeys(const std::string& key) const;

    std::string GetKey(const std::string& key, bool throw_if_not_exists = true,
                       std::string default_value = {}) const;

    void PutKey(const std::string& key, const std::string& content) const;

    const std::string& url() const { return url_; }
    const std::string& prefix() const { return prefix_; }

  private:
    std::string url_;
    std::string prefix_;
};

}}}

#endif // VIYA_CLUSTER_CONSUL_CONSUL_H_
