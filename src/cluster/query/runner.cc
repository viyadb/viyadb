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

#include "cluster/query/runner.h"
#include "cluster/controller.h"
#include "cluster/query/agg_runner.h"
#include "cluster/query/load_runner.h"
#include "cluster/query/local_runner.h"
#include "cluster/query/search_runner.h"

namespace viya {
namespace cluster {
namespace query {

ClusterQueryProcessor::ClusterQueryProcessor(Controller &controller,
                                             WorkersStates &workers_states,
                                             query::RowOutput &output)
    : controller_(controller), workers_states_(workers_states),
      output_(output) {}

void ClusterQueryProcessor::Visit(const RemoteQuery *remote_query) const {
  auto query_type = remote_query->query().str("type");

  if (query_type == "aggregate") {
    AggQueryRunner runner(controller_, workers_states_, output_);
    runner.Run(remote_query);

  } else if (query_type == "search") {
    SearchQueryRunner runner(controller_, workers_states_, output_);
    runner.Run(remote_query);

  } else {
    throw std::runtime_error("unsupported query type");
  }
}

void ClusterQueryProcessor::Visit(const LocalQuery *query) const {
  LocalQueryRunner runner(controller_, output_);
  runner.Run(query);
}

void ClusterQueryProcessor::Visit(const LoadQuery *query) const {
  LoadQueryRunner runner(controller_, workers_states_, output_);
  runner.Run(query);
}

ClusterQueryRunner::ClusterQueryRunner(Controller &controller)
    : controller_(controller) {}

void ClusterQueryRunner::Run(const ClusterQuery &query,
                             query::RowOutput &output) {
  ClusterQueryProcessor processor(controller_, workers_states_, output);
  query.Accept(processor);
}

} // namespace query
} // namespace cluster
} // namespace viya
