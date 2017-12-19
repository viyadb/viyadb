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

#ifndef VIYA_CLUSTER_CONSUL_SERVICE_H_
#define VIYA_CLUSTER_CONSUL_SERVICE_H_

#include <memory>
#include <json.hpp>
#include "util/schedule.h"

namespace viya {
namespace cluster {
namespace consul {

using json = nlohmann::json;

class Consul;

class Service {
  enum Status { OK, FAIL, WARN };

  public:
    Service(const Consul& consul, const std::string& name, uint16_t port, uint32_t ttl_sec, bool auto_hc);
    Service(const Service& other) = delete;
    ~Service();

    void Notify(Status status, const std::string& message);

  private:
    void Register();
    void Deregister();

  private:
    const Consul& consul_;
    const std::string name_;
    const uint16_t port_;
    const uint32_t ttl_sec_;
    std::string id_;
    json data_;
    std::unique_ptr<util::Repeat> repeat_;
};

}}}

#endif // VIYA_CLUSTER_CONSUL_SERVICE_H_
