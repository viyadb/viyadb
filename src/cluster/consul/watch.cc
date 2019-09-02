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
#include "util/base64.h"
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
      response = std::make_unique<json>(json::parse(r.text));
      index_ = std::stol(r.header["x-consul-index"]);
      break;
    case 404:
      // Key doesn't exist yet - return empty result
      break;
    case 0:
      throw std::runtime_error("Can't contact Consul at: " + url_ +
                               " (host is unreachable)");
    default:
      throw std::runtime_error("Can't watch key (" + r.text + ")");
    }
  }
  return response;
}

std::vector<Watch::UpdatedKey> Watch::GetUpdatedKeys(int32_t timeout) {
  auto result_json = LastChanges(timeout);
  if (!result_json) {
    return std::vector<UpdatedKey>{};
  }
  auto result_list = result_json->get<std::vector<json>>();
  std::vector<UpdatedKey> keys(result_list.size());
  auto out = keys.begin();
  for (auto in = result_list.begin(); in != result_list.end(); ++in, ++out) {
    auto key = (*in)["Key"].get<std::string>();
    out->key = key.erase(0, key.rfind("/") + 1);
    auto value_base64 = (*in)["Value"];
    if (value_base64 != nullptr) {
      out->value = base64_decode(value_base64);
    }
    out->session = (*in)["Session"];
    out->modify_index = (*in)["ModifyIndex"];
  }
  return keys;
}

} // namespace consul
} // namespace cluster
} // namespace viya
