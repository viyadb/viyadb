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

#include "cluster/consul/watch.h"
#include "cluster/consul/consul.h"
#include <cpr/cpr.h>
#include <glog/logging.h>

namespace viya {
namespace cluster {
namespace consul {

Watch::Watch(const Consul &consul, const std::string &key, bool recurse)
    : consul_(consul), key_(consul.prefix() + "/" + key), recurse_(recurse),
      url_(consul_.url() + "/v1/kv/" + key_), index_(1) {}

std::unique_ptr<json> Watch::LastChanges(int32_t timeout) {
  DLOG(INFO) << "Opening blocking connection to URL: " << url_;
  cpr::Parameters params{{"index", std::to_string(index_)}};
  if (recurse_) {
    params.AddParameter({"recurse", "true"});
  }
  auto r = cpr::Get(cpr::Url{url_}, params, cpr::Timeout{timeout});
  std::unique_ptr<json> response{};
  if (r.error.code != cpr::ErrorCode::OPERATION_TIMEDOUT) {
    switch (r.status_code) {
    case 200:
      break;
    case 0:
      throw std::runtime_error("Can't contact Consul at: " + url_ +
                               " (host is unreachable)");
    case 404:
      throw std::runtime_error("Key doesn't exist: " + key_);
    default:
      throw std::runtime_error("Can't watch key (" + r.text + ")");
    }
    response = std::make_unique<json>(json::parse(r.text)[0].get<json>());
    index_ = (*response)["ModifyIndex"].get<long>();
  }
  return response;
}

} // namespace consul
} // namespace cluster
} // namespace viya
