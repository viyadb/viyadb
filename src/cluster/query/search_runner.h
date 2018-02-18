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

#ifndef VIYA_CLUSTER_QUERY_SEARCH_RUNNER_H_
#define VIYA_CLUSTER_QUERY_SEARCH_RUNNER_H_

#include "cluster/query/query.h"

namespace viya {
namespace cluster {

class Controller;

} // namespace cluster
} // namespace viya

namespace viya {
namespace query {

class RowOutput;

} // namespace query
} // namespace viya

namespace viya {
namespace cluster {
namespace query {

class WorkersStates;

namespace util = viya::util;
namespace query = viya::query;

class SearchQueryRunner {
public:
  SearchQueryRunner(Controller &controller, WorkersStates &workers_states,
                    query::RowOutput &output);

  void Run(const RemoteQuery *query);

private:
  Controller &controller_;
  WorkersStates &workers_states_;
  query::RowOutput &output_;
};

} // namespace query
} // namespace cluster
} // namespace viya

#endif // VIYA_CLUSTER_QUERY_SEARCH_RUNNER_H_
