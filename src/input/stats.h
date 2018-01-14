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

#ifndef VIYA_INPUT_STATS_H_
#define VIYA_INPUT_STATS_H_

#include "db/stats.h"
#include <chrono>
#include <cstddef>
#include <string>

namespace viya {
namespace util {
class Statsd;
}
} // namespace viya

namespace viya {
namespace input {

namespace db = viya::db;
namespace cr = std::chrono;
namespace util = viya::util;

class LoaderStats {
public:
  LoaderStats(const util::Statsd &statsd, const std::string &table)
      : statsd_(statsd), table_(table), total_recs(0), failed_recs(0) {}

  void OnBegin();
  void OnEnd();

public:
  const util::Statsd &statsd_;
  std::string table_;
  size_t total_recs;
  size_t failed_recs;
  cr::duration<float> whole_time;
  db::UpsertStats upsert_stats;

private:
  cr::steady_clock::time_point begin_work_;
};
} // namespace input
} // namespace viya

#endif // VIYA_INPUT_STATS_H_
