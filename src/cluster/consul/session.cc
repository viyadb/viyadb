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

#include <stdexcept>
#include <json.hpp>
#include <cpr/cpr.h>
#include <glog/logging.h>
#include "cluster/consul/consul.h"

namespace viya {
namespace cluster {
namespace consul {

using json = nlohmann::json;

Session::Session(const Consul& consul, const std::string& name, uint32_t ttl_sec):
  consul_(consul),name_(name),ttl_sec_(ttl_sec),repeat_(nullptr) {

  Create();

  repeat_ = std::make_unique<util::Repeat>((ttl_sec_ * 2000L) / 3, [this]() {
    Renew();
  });
}

Session::~Session() {
  repeat_.reset();
  Destroy();
}

void Session::Destroy() {
  LOG(INFO)<<"Destroying session '"<<id_<<"'";
  cpr::Put(
    cpr::Url { consul_.url() + "/v1/session/destroy/" + id_ },
    cpr::Timeout { 3000L }
  );
}

void Session::Create() {
  json data = {
    { "Name", name_ },
    { "Checks", json::array() },
    { "LockDelay", "0s" },
    { "Behavior", "delete" },
    { "TTL", std::to_string(ttl_sec_) + "s" }
  };

  DLOG(INFO)<<"Creating new session '"<<name_<<"' with TTL="<<ttl_sec_<<"sec";
  auto r = cpr::Put(
    cpr::Url { consul_.url() + "/v1/session/create" },
    cpr::Body { data.dump() },
    cpr::Header {{ "Content-Type", "application/json" }},
    cpr::Timeout { 3000L }
  );
  if (r.status_code != 200) {
    if (r.status_code == 0) {
      throw std::runtime_error("Can't contact Consul (host is unreachable)");
    }
    throw std::runtime_error("Can't register new session (" + r.text + ")");
  }

  json response = json::parse(r.text);
  id_ = response["ID"].get<std::string>();

  DLOG(INFO)<<"Created new session: "<<id_;
}

void Session::Renew() {
  DLOG(INFO)<<"Renewing session '"<<id_<<"'";
  auto r = cpr::Put(
    cpr::Url { consul_.url() + "/v1/session/renew/" + id_ },
    cpr::Timeout { 3000L }
  );

  if (r.status_code == 0) {
    LOG(WARNING)<<"Can't contact Consul (host is unreachable)";
  }
  else if (r.status_code == 404) {
    LOG(WARNING)<<"Session '"<<id_<<"' was invalidated externally";
    Create();
  }
}

bool Session::EphemeralKey(const std::string& key, const std::string& value) const {
  std::string target_key = consul_.prefix() + "/" + key;
  DLOG(INFO)<<"Acquiring lock on key '"<<target_key<<"' for session '"<<id_;

  auto r = cpr::Put(
    cpr::Url { consul_.url() + "/v1/kv/" + target_key },
    cpr::Body { value },
    cpr::Parameters {{ "acquire", id_ }},
    cpr::Header {{ "Content-Type", "application/json" }},
    cpr::Timeout { 3000L }
  );

  if (r.status_code != 200) {
    if (r.status_code == 0) {
      throw std::runtime_error("Can't contact Consul (host is unreachable)");
    }
    throw std::runtime_error("Can't acquire lock on key (" + r.text + ")");
  }
  return r.text.compare(0, 4, "true") == 0;
}

}}}
