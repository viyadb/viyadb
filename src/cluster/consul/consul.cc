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

#include "cluster/consul/consul.h"
#include <algorithm>
#include <cpr/cpr.h>
#include <glog/logging.h>
#include <stdexcept>

namespace viya {
namespace cluster {
namespace consul {

Consul::Consul(const util::Config &config) {
  url_ = config.str("consul_url");
  url_.erase(std::find_if(url_.rbegin(), url_.rend(),
                          [](unsigned char ch) { return ch != '/'; })
                 .base(),
             url_.end());

  prefix_ = config.str("consul_prefix", "viyadb");
}

std::unique_ptr<Session> Consul::CreateSession(const std::string &name,
                                               uint32_t ttl_sec) const {
  return std::make_unique<Session>(*this, name, ttl_sec);
}

std::unique_ptr<Service> Consul::RegisterService(const std::string &name,
                                                 uint16_t port,
                                                 uint32_t ttl_sec,
                                                 bool auto_hc) const {
  return std::make_unique<Service>(*this, name, port, ttl_sec, auto_hc);
}

std::unique_ptr<LeaderElector>
Consul::ElectLeader(const Session &session, const std::string &key) const {
  return std::make_unique<LeaderElector>(*this, session, key);
}

std::unique_ptr<Watch> Consul::WatchKey(const std::string &key,
                                        bool recurse) const {
  return std::make_unique<Watch>(*this, key, recurse);
}

std::vector<std::string> Consul::ListKeys(const std::string &key) const {
  auto r = cpr::Get(cpr::Url{url_ + "/v1/kv/" + prefix_ + "/" + key},
                    cpr::Parameters{{"keys", "true"}}, cpr::Timeout{3000L});
  switch (r.status_code) {
  case 200:
    break;
  case 0:
    throw std::runtime_error("Can't contact Consul at: " + url_ +
                             " (host is unreachable)");
  case 404:
    return std::move(std::vector<std::string>{});
  default:
    throw std::runtime_error("Can't list keys (" + r.text + ")");
  }
  auto keys = json::parse(r.text).get<std::vector<std::string>>();
  std::for_each(keys.begin(), keys.end(),
                [](std::string &key) { key.erase(0, key.rfind("/") + 1); });
  return std::move(keys);
}

std::string Consul::GetKey(const std::string &key, bool throw_if_not_exists,
                           std::string default_value) const {
  auto r = cpr::Get(cpr::Url{url_ + "/v1/kv/" + prefix_ + "/" + key},
                    cpr::Parameters{{"raw", "true"}}, cpr::Timeout{3000L});
  switch (r.status_code) {
  case 200:
    break;
  case 0:
    throw std::runtime_error("Can't contact Consul at: " + url_ +
                             " (host is unreachable)");
  case 404:
    if (throw_if_not_exists) {
      throw std::runtime_error("Key doesn't exist: " + prefix_ + "/" + key);
    }
    return default_value;
  default:
    throw std::runtime_error("Can't get key contents (" + r.text + ")");
  }
  return r.text;
}

void Consul::PutKey(const std::string &key, const std::string &content) const {
  auto r = cpr::Put(cpr::Url{url_ + "/v1/kv/" + prefix_ + "/" + key},
                    cpr::Body{content}, cpr::Timeout{3000L});
  if (r.status_code != 200) {
    if (r.status_code == 0) {
      throw std::runtime_error("Can't contact Consul at: " + url_ +
                               " (host is unreachable)");
    }
    throw std::runtime_error("Can't put key contents (" + r.text + ")");
  }
}
}
}
}
