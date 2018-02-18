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

#include "cluster/query/local_runner.h"
#include "cluster/controller.h"
#include "cluster/query/query.h"
#include "query/output.h"
#include "query/query.h"

namespace viya {
namespace cluster {
namespace query {

namespace input = viya::input;

LocalQueryRunner::LocalQueryRunner(Controller &controller,
                                   query::RowOutput &output)
    : controller_(controller), output_(output) {}

void LocalQueryRunner::ShowWorkers() {
  query::RowOutput::Row workers;
  // TODO: implement me
  output_.Start();
  output_.SendAsCol(workers);
  output_.Flush();
}

void LocalQueryRunner::Run(const LocalQuery *local_query) {
  auto &query = local_query->query();
  if (query.str("type") == "show" && query.str("what") == "workers") {
    ShowWorkers();
  } else {
    controller_.db().Query(query, output_);
  }
}

} // namespace query
} // namespace cluster
} // namespace viya
