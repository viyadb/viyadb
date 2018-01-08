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

#ifndef VIYA_CLUSTER_CONSUL_SESSION_H_
#define VIYA_CLUSTER_CONSUL_SESSION_H_

#include <memory>
#include <functional>
#include "util/schedule.h"

namespace viya {
namespace cluster {
namespace consul {

class Consul;

class Session {
  public:
    Session(const Consul& consul, const std::string& name, uint32_t ttl_sec);
    Session(const Session& other) = delete;
    ~Session();

    void Renew();
    bool EphemeralKey(const std::string& key, const std::string& value) const;

    const std::string& id() const { return id_; }

  private:
    void Destroy();
    void Create();

  private:
    const Consul& consul_;
    const std::string name_;
    const uint32_t ttl_sec_;
    std::string id_;
    std::unique_ptr<util::Repeat> repeat_;
};

}}}

#endif // VIYA_CLUSTER_CONSUL_SESSION_H_
