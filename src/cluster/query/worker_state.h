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

#ifndef VIYA_CLUSTER_QUERY_WORKER_STATE_H_
#define VIYA_CLUSTER_QUERY_WORKER_STATE_H_

#include <cstdint>
#include <ctime>
#include <mutex>
#include <unordered_map>

namespace viya {
namespace cluster {
namespace query {

class WorkerState {
public:
  WorkerState(uint32_t refresh_sec = 10L)
      : failing_(false), last_update_(0L), refresh_sec_(refresh_sec) {}

  bool IsFailing();
  void SetFailing();

private:
  bool failing_;
  std::time_t last_update_;
  uint32_t refresh_sec_;
};

class WorkersStates {
public:
  WorkersStates() {}

  bool IsFailing(const std::string &worker_id);
  void SetFailing(const std::string &worker_id);

private:
  std::unordered_map<std::string, WorkerState> states_;
  std::mutex mutex_;
};

} // namespace query
} // namespace cluster
} // namespace viya

#endif // VIYA_CLUSTER_QUERY_WORKER_STATE_H_
