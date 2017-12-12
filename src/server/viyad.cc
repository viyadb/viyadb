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

#include <sys/types.h>
#include <unistd.h>
#include <sched.h>
#include <json.hpp>
#include "db/database.h"
#include "server/http/service.h"
#include "server/viyad.h"

namespace viya {
namespace server {

namespace db = viya::db;
namespace cluster = viya::cluster;
namespace server = viya::server;

using json = nlohmann::json;

Viyad::Viyad(const util::Config& config):config_(config) {
}

void Viyad::Start() {
#ifdef __linux__
  SetCpuAffinity();
#endif

  db::Database database(config_);
  Http http_service(config_, database);
  
  if (config_.exists("cluster_id")) {
    worker_ = std::make_unique<cluster::Worker>(config_);
  }
  http_service.Start();
}

#ifdef __linux__
void Viyad::SetCpuAffinity() {
  if (config_.exists("cpu_list")) {
    pid_t pid= getpid();
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    for (auto cpu : config_.numlist("cpu_list")) {
      CPU_SET(cpu, &cpu_set);
    }
    sched_setaffinity(pid, sizeof(cpu_set), &cpu_set);
  }
}
#endif

}}
