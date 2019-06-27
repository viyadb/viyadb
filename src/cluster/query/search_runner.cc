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

#include "cluster/query/search_runner.h"
#include "cluster/query/client.h"
#include "cluster/query/query.h"
#include "cluster/query/worker_state.h"
#include "query/output.h"
#include "query/query.h"
#include <algorithm>
#include <cstring>

namespace viya {
namespace cluster {
namespace query {

SearchQueryRunner::SearchQueryRunner(WorkersStates &workers_states,
                                     query::RowOutput &output)
    : workers_states_(workers_states), output_(output) {}

void SearchQueryRunner::Run(const RemoteQuery *remote_query) {
  auto &target_workers = remote_query->target_workers();
  if (target_workers.empty()) {
    throw std::runtime_error("query must contain clustering key in its filter");
  }

  auto search_query = remote_query->query();

  size_t limit = search_query.num("limit", 0);
  query::RowOutput::Row result;

  WorkersClient http_client(workers_states_, [&result, limit](const char *buf,
                                                              size_t buf_size) {
    if (buf_size > 0) {
      const char *buf_end = buf + buf_size;
      for (const char *lstart = buf;
           lstart < buf_end && (limit == 0 || result.size() < limit);) {
        const char *lend = (const char *)memchr(lstart, '\n', buf_end - lstart);
        if (lend == NULL) {
          lend = buf_end;
        }
        result.emplace_back(lstart, lend - lstart);
        lstart = lend + 1;
      }
    }
  });

  auto query_data = search_query.dump();
  for (auto &replicas : target_workers) {
    auto randomized_workers = replicas;
    std::random_shuffle(randomized_workers.begin(), randomized_workers.end());
    http_client.Send(randomized_workers, "/query", query_data);
  }
  http_client.Await();

  output_.Start();
  output_.SendAsCol(result);
  output_.Flush();
}

} // namespace query
} // namespace cluster
} // namespace viya
