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

#include "cluster/query/worker_state.h"

namespace viya {
namespace cluster {
namespace query {

bool WorkerState::IsFailing() {
  if (failing_ && std::time(nullptr) - last_update_ > refresh_sec_) {
    failing_ = false;
  }
  return failing_;
}

void WorkerState::SetFailing() {
  failing_ = true;
  last_update_ = std::time(nullptr);
}

bool WorkersStates::IsFailing(const std::string &worker_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = states_.find(worker_id);
  return it != states_.end() && it->second.IsFailing();
}

void WorkersStates::SetFailing(const std::string &worker_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto s = states_.emplace(worker_id, WorkerState{});
  s.first->second.SetFailing();
}

} // namespace query
} // namespace cluster
} // namespace viya
