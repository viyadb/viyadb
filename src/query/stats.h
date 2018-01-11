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

#ifndef VIYA_QUERY_STATS_H_
#define VIYA_QUERY_STATS_H_

#include <chrono>
#include <string>

namespace viya {
namespace util {
class Statsd;
}
}

namespace viya {
namespace query {

namespace cr = std::chrono;
namespace util = viya::util;

class QueryStats {
public:
  QueryStats(const util::Statsd &statsd)
      : statsd_(statsd), scanned_segments(0), scanned_recs(0),
        aggregated_recs(0), output_recs(0) {}

  void OnBegin(const std::string &query_type, const std::string &table);
  void OnCompile();
  void OnEnd();

public:
  const util::Statsd &statsd_;
  std::string query_type_;
  std::string table_;
  size_t scanned_segments;
  size_t scanned_recs;
  size_t aggregated_recs;
  size_t output_recs;
  cr::duration<float> compile_time;
  cr::duration<float> whole_time;

private:
  cr::steady_clock::time_point begin_work_;
};
}
}

#endif // VIYA_QUERY_STATS_H_
