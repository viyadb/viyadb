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

#ifndef VIYA_SERVER_SUPERVISOR_H_
#define VIYA_SERVER_SUPERVISOR_H_

#include <memory>
#include <sys/types.h>
#include <vector>
#include <csetjmp>
#include "cluster/controller.h"
#include "util/config.h"

namespace viya {
namespace server {

namespace util = viya::util;
namespace cluster = viya::cluster;

class Supervisor {
  public:
    struct Worker {
      pid_t pid;
      uint64_t last_start;
      uint8_t fast_failures = 0;
      util::Config config;
    };

  public:
    Supervisor(const std::vector<std::string>& args);

    void Start();
    void Stop();
    void Restart();

  private:
    void EnableRestartHandler();
    void EnableStopHandler();
    util::Config PrepareWorkerConfig(const util::Config& config, size_t worker_idx);
    void StartWorker(util::Config worker_config, Supervisor::Worker& info);

  private:
    static Supervisor* instance_;
    std::vector<std::string> args_;
    std::vector<Worker> workers_;
    std::unique_ptr<cluster::Controller> controller_;
    std::jmp_buf jump_;
};

}}

#endif // VIYA_SERVER_SUPERVISOR_H_
