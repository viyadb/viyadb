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

#include <chrono>
#include <json.hpp>
#include <glog/logging.h>
#include "cluster/worker.h"
#include "util/hostname.h"

namespace viya {
namespace cluster {

using json = nlohmann::json;

Worker::Worker(const util::Config& config):config_(config),consul_(config) {
  session_ = consul_.CreateSession(std::string("viyadb-worker"));
  CreateKey();
}

void Worker::CreateKey() const {
  std::string hostname = util::get_hostname();
  auto worker_key = "clusters/" + config_.str("cluster_id")
     + "/nodes/workers/" + hostname + ":" + std::to_string(config_.num("http_port"));

  json data = json({});
  if (config_.exists("rack_id")) {
    data["rack_id"] = config_.str("rack_id");
  }
  if (config_.exists("cpu_list")) {
    data["cpu_list"] = config_.numlist("cpu_list");
  }
  data["http_port"] = config_.num("http_port");
  data["hostname"] = hostname;

  while (!session_->EphemeralKey(worker_key, data.dump())) {
    LOG(WARNING)<<"The worker key is still locked by the previous process... waiting";
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  }
}

}}
