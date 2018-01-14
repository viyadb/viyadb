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

#include "input/stats.h"
#include "util/statsd.h"
#include <glog/logging.h>
#include <sstream>

namespace viya {
namespace input {

void LoaderStats::OnBegin() { begin_work_ = cr::steady_clock::now(); }

void LoaderStats::OnEnd() {
  whole_time = cr::steady_clock::now() - begin_work_;
  auto load_time = cr::duration_cast<cr::milliseconds>(whole_time).count();

  LOG(INFO) << "Load time " << load_time << " ms ("
            << "tr=" << std::to_string(total_recs)
            << ",fr=" << std::to_string(failed_recs)
            << ",nr=" << std::to_string(upsert_stats.new_recs) << ")"
            << std::endl;

  std::ostringstream prefix;
  prefix << "loader." << table_ << ".";
  statsd_.Timing(prefix.str() + "time", load_time);
  statsd_.Count(prefix.str() + "rows", total_recs);
}
} // namespace input
} // namespace viya
