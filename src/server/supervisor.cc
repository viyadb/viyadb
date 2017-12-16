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
#include <sys/wait.h>
#include <signal.h>
#include <stdexcept>
#include <vector>
#include <chrono>
#include <csetjmp>
#if __linux__
 #include <sys/prctl.h>
#endif
#include <glog/logging.h>
#include <boost/exception/diagnostic_information.hpp>
#include "server/args.h"
#include "server/supervisor.h"
#include "server/viyad.h"

namespace viya {
namespace server {

namespace chrono = std::chrono;

static server::Supervisor* supervisor_ = nullptr;

Supervisor::Supervisor(const std::vector<std::string>& args):args_(args) {
  supervisor_ = this;
}

void restart(int signal __attribute__((unused))) {
  if (supervisor_ != nullptr) {
    supervisor_->Restart();
  }
}

void Supervisor::EnableRestartHandler() {
  struct sigaction sig_action;
  sig_action.sa_handler = restart;
  sigemptyset(&sig_action.sa_mask);
  sig_action.sa_flags = SA_RESETHAND;
  sigaction(SIGHUP, &sig_action, NULL);
}

void stop(int signal __attribute__((unused))) {
  exit(0);
}

void Supervisor::EnableStopHandler() {
  struct sigaction sig_action;
  sig_action.sa_handler = stop;
  sigemptyset(&sig_action.sa_mask);
  sig_action.sa_flags = SA_RESETHAND;
  sigaction(SIGINT, &sig_action, NULL);
}

void Supervisor::Start() {
  sigsetjmp(jump_, 1);

  EnableStopHandler();

  util::Config config = server::CmdlineArgs().Parse(args_);
  LOG(INFO)<<"Using configuration:\n"<<config.dump();

  size_t workers_num = config.num("workers");
  if (!config.boolean("supervise")) {
    Viyad viyad(config);
    viyad.Start();
    return;
  }

  EnableRestartHandler();

  if (config.exists("cluster_id")) {
    controller_.reset(new cluster::Controller(config));
  }

  auto cpu_list = config.numlist("cpu_list");
  if (cpu_list.size() < workers_num) {
    throw std::invalid_argument("Number of available CPU is less than number of workers!");
  }
  if (cpu_list.size() % workers_num != 0) {
    throw std::invalid_argument("Number of workers must be a multiplies of CPU number!");
  }

  LOG(INFO)<<"Starting "<<workers_num<<" workers";
  workers_.resize(workers_num);
  for (size_t worker_idx = 0; worker_idx < workers_num; ++worker_idx) {
    auto worker_config = PrepareWorkerConfig(config, worker_idx);
    StartWorker(worker_config, workers_[worker_idx]);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  uint8_t max_failures = 3;
  uint64_t start_fail_secs = 5;

  while (true) {
    int status;
    pid_t exited = wait(&status);

    if (exited == -1) {
      if (errno == ECHILD) {
        throw std::runtime_error("We lost all of the workers!");
      }
    } else {
      if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {

        for (auto it = workers_.begin(); it != workers_.end(); ++it) {
          if (it->pid == exited) {

            auto now = chrono::duration_cast<chrono::seconds>(chrono::system_clock::now().time_since_epoch()).count();
            if (now - it->last_start < start_fail_secs) {
              ++it->fast_failures;
            }

            int worker_idx = it - workers_.begin();
            if (it->fast_failures < max_failures) {

              LOG(ERROR)<<"Worker #"<<std::to_string(worker_idx)<<" has exited with bad exit status ("
                <<std::to_string(status)<<"). Restarting it in a moment.";

              StartWorker(workers_[worker_idx].config, workers_[worker_idx]);
            } else {
              LOG(ERROR)<<"Worker #"<<std::to_string(worker_idx)<<" failed too fast for "
                <<std::to_string(max_failures)<<" times in a row. Not restarting it any more.";
            }
            break;
          }
        }
      }
    }
  }
}

void Supervisor::Stop() {
  LOG(INFO)<<"Stopping all workers";
  for (auto& worker : workers_) {
    kill(worker.pid, SIGTERM);
  }

  int status;
  while (waitpid(-1, &status, WNOHANG | WUNTRACED) != -1) {
    // wait for the workers
  }
}

void Supervisor::Restart() {
  Stop();
  siglongjmp(jump_, 0);
}

util::Config Supervisor::PrepareWorkerConfig(const util::Config& config, size_t worker_idx) {
  util::Config worker_config = config;

  worker_config.set_num("http_port", config.num("http_port") + worker_idx);

  // Calculate list of CPU to allocate for the worker:
  size_t workers_num = config.num("workers");
  auto cpu_list = config.numlist("cpu_list");
  size_t worker_cpu_num = cpu_list.size() / workers_num;
  std::vector<long> worker_cpu_list(worker_cpu_num);
  for (size_t i = 0; i < worker_cpu_num; ++i) {
    worker_cpu_list[i] = cpu_list[worker_cpu_num * worker_idx + i];
  }
  worker_config.set_numlist("cpu_list", worker_cpu_list);

  return worker_config;
}

void Supervisor::StartWorker(util::Config worker_config, Supervisor::Worker& info) {
  pid_t pid = fork();
  if (pid == -1) {
    throw std::runtime_error("Can't fork any more processes!");
  }

  if (pid == 0) { // inside child worker
    workers_.clear();

    // Reset all signals to default behavior
    struct sigaction sig_action;
    sig_action.sa_handler = SIG_DFL;
    sig_action.sa_flags = 0;
    sigemptyset(&sig_action.sa_mask);
    for (int sig = 1; sig < NSIG ; sig++) {
      if (sig != SIGSEGV && sig != SIGABRT) {
        sigaction(sig, &sig_action, NULL);
      }
    }

    prctl(PR_SET_PDEATHSIG, SIGHUP);
    try {
      Viyad viyad(worker_config);
      viyad.Start();
    } catch (...) {
      LOG(ERROR)<<"Exception thrown in worker: "<<boost::current_exception_diagnostic_information();
      exit(1);
    }
    exit(0);
  }

  // Update worker info to latest settings:
  info.config = worker_config;
  info.pid = pid;
  info.last_start = chrono::duration_cast<chrono::seconds>(chrono::system_clock::now().time_since_epoch()).count();
}

}}
