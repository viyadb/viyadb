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

#ifndef VIYA_CLUSTER_CONSUL_WATCH_H_
#define VIYA_CLUSTER_CONSUL_WATCH_H_

#include <json.hpp>

namespace viya {
namespace cluster {
namespace consul {

using json = nlohmann::json;

class Consul;

class Watch {
  public:
    Watch(const Consul& consul, const std::string& key);
    Watch(const Watch& other) = delete;

    json LastChanges();

  private:
    const Consul& consul_;
    const std::string key_;
    const std::string url_;
    long index_;
};

}}}

#endif // VIYA_CLUSTER_CONSUL_WATCH_H_
