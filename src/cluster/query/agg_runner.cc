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

#include "cluster/query/agg_runner.h"
#include "cluster/controller.h"
#include "cluster/query/client.h"
#include "cluster/query/query.h"
#include "cluster/query/worker_state.h"
#include "input/buffer_loader.h"
#include "query/output.h"
#include "query/query.h"
#include "util/config.h"
#include "util/scope_guard.h"
#include <nlohmann/json.hpp>
#include <random>

namespace viya {
namespace cluster {
namespace query {

using json = nlohmann::json;

namespace input = viya::input;

AggQueryRunner::AggQueryRunner(Controller &controller,
                               WorkersStates &workers_states,
                               query::RowOutput &output)
    : controller_(controller), workers_states_(workers_states),
      output_(output) {}

std::string AggQueryRunner::CreateTempTable(const util::Config &query) {
  std::string tmp_table = "query-" + std::to_string(std::rand());

  // Create table descriptor based on the query and the original table:
  query::QueryFactory query_factory;
  std::unique_ptr<query::AggregateQuery> table_query(
      reinterpret_cast<query::AggregateQuery *>(
          query_factory.Create(query, controller_.db())));

  json *orig_conf = static_cast<json *>(
      controller_.tables_configs().at(query.str("table")).json_ptr());
  json dimensions = json::array();
  auto query_columns = table_query->column_names();

  for (auto &it : (*orig_conf)["dimensions"]) {
    auto dim_name = it["name"];
    if (std::find(query_columns.begin(), query_columns.end(), dim_name) !=
        query_columns.end()) {
      dimensions.push_back(it);
    }
  }

  json metrics = json::array();
  for (auto &it : (*orig_conf)["metrics"]) {
    auto metric_name = it["name"];
    if (std::find(query_columns.begin(), query_columns.end(), metric_name) !=
        query_columns.end()) {
      metrics.push_back(it);
    }
  }

  util::Config table_conf(json{
      {"name", tmp_table}, {"dimensions", dimensions}, {"metrics", metrics}});
  controller_.db().CreateTable(tmp_table, table_conf);

  return tmp_table;
}

util::Config AggQueryRunner::CreateWorkerQuery(const util::Config &agg_query) {
  util::Config worker_query = agg_query;
  worker_query.erase("header");
  worker_query.erase("having");
  worker_query.erase("sort");
  worker_query.erase("skip");
  worker_query.erase("limit");
  return worker_query;
}

void AggQueryRunner::Run(const RemoteQuery *remote_query) {
  auto &target_workers = remote_query->target_workers();

  if (target_workers.empty()) {
    throw std::runtime_error("query must contain clustering key in its filter");
  }

  auto agg_query = remote_query->query();
  auto worker_query = CreateWorkerQuery(agg_query);

  auto tmp_table = CreateTempTable(worker_query);
  util::ScopeGuard cleanup = [this, &tmp_table]() {
    controller_.db().DropTable(tmp_table);
  };

  auto table = controller_.db().GetTable(tmp_table);
  std::vector<std::string> columns;
  for (auto col : table->columns()) {
    columns.push_back(col->name());
  }
  util::Config load_desc;
  load_desc.set_str("format", "tsv");
  load_desc.set_strlist("columns", columns);

  WorkersClient http_client(
      workers_states_, [&load_desc, table](const char *buf, size_t buf_size) {
        if (buf_size > 0) {
          input::BufferLoader loader(load_desc, *table, buf, buf_size);
          loader.LoadData();
        }
      });

  auto query_data = worker_query.dump();
  for (auto &replicas : target_workers) {
    auto randomized_workers = replicas;
    std::random_shuffle(randomized_workers.begin(), randomized_workers.end());
    http_client.Send(randomized_workers, "/query", query_data);
  }
  http_client.Await();

  util::Config own_query = agg_query;
  own_query.set_str("table", tmp_table);
  own_query.erase("filter");
  controller_.db().Query(own_query, output_);
}

} // namespace query
} // namespace cluster
} // namespace viya
