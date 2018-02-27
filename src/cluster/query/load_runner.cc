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

#include "cluster/query/load_runner.h"
#include "cluster/controller.h"
#include "cluster/query/client.h"
#include "cluster/query/query.h"
#include "cluster/query/worker_state.h"
#include "query/query.h"
#include "util/config.h"
#include "util/scope_guard.h"
#include <unordered_set>

namespace viya {
namespace cluster {
namespace query {

LoadQueryRunner::LoadQueryRunner(Controller &controller,
                                 WorkersStates &workers_states,
                                 query::RowOutput &output)
    : controller_(controller), workers_states_(workers_states),
      output_(output) {}

void LoadQueryRunner::Run(const LoadQuery *load_query) {

  auto &load_desc = load_query->query();

  WorkersClient http_client(workers_states_,
                            [](const char *buf __attribute__((unused)),
                               size_t buf_size __attribute__((unused))) {});

  std::unordered_set<std::string> controller_ids;
  auto &table_plan = controller_.tables_plans().at(load_desc.str("table"));
  for (auto &parts_it : table_plan.workers_partitions()) {
    auto worker_id = parts_it.first;
    auto controller_id =
        worker_id.substr(0, worker_id.find(":")) + ":" +
        std::to_string(controller_.cluster_config().num("http_port"));
    controller_ids.emplace(controller_id);
  }

  auto data = load_desc.dump();
  for (const auto &controller_id : controller_ids) {
    http_client.Send(std::vector<std::string>{controller_id}, "/load", data);
  }
  http_client.Await();

  output_.Start();
  output_.Flush();
}

} // namespace query
} // namespace cluster
} // namespace viya
