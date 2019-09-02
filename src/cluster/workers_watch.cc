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
#include <chrono>
#include <glog/logging.h>

namespace viya {
namespace cluster {

WorkersWatch::WorkersWatch(const Controller &controller)
    : controller_(controller),
      watch_(controller_.consul().WatchKey(
          "clusters/" + controller_.cluster_id() + "/nodes/workers", true)),
      workers_ready_(1) {

  always_ = std::make_unique<util::Always>([this]() { WatchActiveWorkers(); });
}

void WorkersWatch::WatchActiveWorkers() {
  auto updated_keys = watch_->GetUpdatedKeys();

  if (updated_keys.empty()) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    return;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    workers_configs_.clear();
    for (auto &updated_key : updated_keys) {
      workers_configs_.emplace(updated_key.key, updated_key.value);
    }
    LOG(INFO) << "Read " << workers_configs_.size()
              << " active workers configurations";
  }

  if (updated_keys.size() == controller_.total_workers_num()) {
    workers_ready_.CountDown();
  }
}

void WorkersWatch::WaitForAllWorkers() { workers_ready_.Wait(); }

std::map<std::string, util::Config> WorkersWatch::GetActiveWorkers() {
  std::lock_guard<std::mutex> lock(mutex_);
  return workers_configs_;
}

} // namespace cluster
} // namespace viya
