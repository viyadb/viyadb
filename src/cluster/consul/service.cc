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
#include "util/hostname.h"
#include <cpr/cpr.h>
#include <glog/logging.h>
#include <json.hpp>
#include <stdexcept>

namespace viya {
namespace cluster {
namespace consul {

Service::Service(const Consul &consul, const std::string &name, uint16_t port,
                 uint32_t ttl_sec, bool auto_hc)
    : consul_(consul), name_(name), port_(port), ttl_sec_(ttl_sec),
      id_(name + ":" + std::to_string(port)), repeat_(nullptr) {

  Register();

  if (auto_hc) {
    repeat_ = std::make_unique<util::Repeat>((ttl_sec * 2000L) / 3, [this]() {
      Notify(Status::OK, "Service is alive");
    });
  }
}

Service::~Service() {
  repeat_.reset();
  Deregister();
}

void Service::Deregister() {
  LOG(INFO) << "Deregistering service '" << id_ << "'";
  cpr::Put(cpr::Url{consul_.url() + "/v1/agent/service/deregister/" + id_},
           cpr::Timeout{3000L});
}

void Service::Register() {
  json data = {{"id", id_},
               {"name", name_},
               {"address", util::get_hostname()},
               {"port", port_},
               {"check", {{"ttl", std::to_string(ttl_sec_) + "s"}}}};

  LOG(INFO) << "Registering service '" << id_ << "' with TTL=" << ttl_sec_
            << "sec";
  ;
  auto r = cpr::Put(cpr::Url{consul_.url() + "/v1/agent/service/register"},
                    cpr::Body{data.dump()},
                    cpr::Header{{"Content-Type", "application/json"}},
                    cpr::Timeout{3000L});
  if (r.status_code != 200) {
    if (r.status_code == 0) {
      throw std::runtime_error("Can't contact Consul at: " + consul_.url() +
                               " (host is unreachable)");
    }
    throw std::runtime_error("Can't register new service (" + r.text + ")");
  }
}

void Service::Notify(Status status, const std::string &message) {
  std::string path;
  switch (status) {
  case Status::OK:
    path = "pass";
    break;
  case Status::FAIL:
    path = "fail";
    break;
  case Status::WARN:
    path = "warn";
    break;
  }
  DLOG(INFO) << "Updating '" << id_ << "' service status to '" << path << "'";
  auto r = cpr::Get(
      cpr::Url{consul_.url() + "/v1/agent/check/" + path + "/service:" + id_},
      cpr::Parameters{{"note", message}}, cpr::Timeout{3000L});
  if (r.status_code == 404) {
    LOG(WARNING) << "Service '" << id_ << "' has been deregistered externally";
    Register();
  }
}

} // namespace consul
} // namespace cluster
} // namespace viya
