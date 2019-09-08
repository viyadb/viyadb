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

#include "cluster/workers_watch.h"
#include "cluster/controller.h"
#include <algorithm>
#include <chrono>
#include <glog/logging.h>

namespace viya {
namespace cluster {

WorkersWatch::WorkersWatch(const Controller &controller)
    : controller_(controller),
      watch_(controller_.consul().WatchKey(
          "clusters/" + controller_.cluster_id() + "/nodes/workers", true)),
      workers_ready_(1), workers_initialized_(false) {

  always_ = std::make_unique<util::Always>([this]() { WatchActiveWorkers(); });
}

void WorkersWatch::WatchActiveWorkers() {
  auto updated_keys = watch_->GetUpdatedKeys();

  if (updated_keys.empty()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    return;
  }

  if (workers_configs_.size() > updated_keys.size()) {
    // Number of active workers is less than the known workers number.
    // Some of them are failing -- let's find out which ones:
    for (auto &w : workers_configs_) {
      auto &worker_id = w.first;
      if (std::find_if(updated_keys.begin(), updated_keys.end(),
                       [&worker_id](auto &k) { return k.key == worker_id; }) ==
          updated_keys.end()) {
        if (failing_workers_.insert(worker_id).second) {
          LOG(INFO) << "Worker " << worker_id << " has disappeared";
        }
      }
    }
  }

  std::unordered_set<std::string> recovered_workers;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    workers_configs_.clear();
    for (auto &worker_key : updated_keys) {
      auto &worker_id = worker_key.key;
      workers_configs_.emplace(worker_id, worker_key.value);

      if (failing_workers_.erase(worker_id) > 0) {
        LOG(INFO) << "Worker " << worker_id << " has recovered";
        recovered_workers.insert(worker_id);
      }
    }
    LOG(INFO) << "Read " << workers_configs_.size()
              << " active workers configurations";
  }

  if (!workers_initialized_ &&
      updated_keys.size() == controller_.total_workers_num()) {
    LOG(INFO) << "All workers are ready";
    workers_ready_.CountDown();
    workers_initialized_ = true;
  }

  for (auto &worker_id : recovered_workers) {
    controller_.RecoverWorker(worker_id);
  }
}

void WorkersWatch::WaitForAllWorkers() { workers_ready_.Wait(); }

const std::map<std::string, util::Config> WorkersWatch::GetActiveWorkers() {
  std::lock_guard<std::mutex> lock(mutex_);
  return workers_configs_;
}

} // namespace cluster
} // namespace viya
