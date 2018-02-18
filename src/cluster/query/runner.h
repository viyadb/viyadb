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

#ifndef VIYA_CLUSTER_QUERY_RUNNER_H_
#define VIYA_CLUSTER_QUERY_RUNNER_H_

#include "cluster/query/query.h"
#include "cluster/query/worker_state.h"
#include "util/config.h"
#include <vector>

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

namespace util = viya::util;
namespace query = viya::query;

class ClusterQueryProcessor : public ClusterQueryVisitor {
public:
  ClusterQueryProcessor(Controller &controller, WorkersStates &workers_states,
                        query::RowOutput &output);

  void Visit(const RemoteQuery *query) const override;
  void Visit(const LocalQuery *query) const override;
  void Visit(const LoadQuery *query) const override;

private:
  Controller &controller_;
  WorkersStates &workers_states_;
  query::RowOutput &output_;
};

class ClusterQueryRunner {
public:
  ClusterQueryRunner(Controller &controller);

  void Run(const ClusterQuery &query, query::RowOutput &output);

private:
  Controller &controller_;
  WorkersStates workers_states_;
};

} // namespace query
} // namespace cluster
} // namespace viya

#endif // VIYA_CLUSTER_QUERY_RUNNER_H_
