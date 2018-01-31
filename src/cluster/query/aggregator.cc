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

#include "cluster/query/aggregator.h"
#include "cluster/controller.h"
#include "cluster/query/client.h"
#include "cluster/query/query.h"
#include "cluster/query/worker_state.h"
#include "db/table.h"
#include "input/buffer_loader.h"
#include "query/output.h"
#include "query/query.h"
#include "util/config.h"
#include "util/scope_guard.h"
#include <glog/logging.h>
#include <json.hpp>
#include <random>

namespace viya {
namespace cluster {
namespace query {

using json = nlohmann::json;

namespace input = viya::input;

Aggregator::Aggregator(Controller &controller, WorkersStates &workers_states,
                       query::RowOutput &output)
    : controller_(controller), workers_states_(workers_states),
      output_(output) {}

std::string Aggregator::CreateTempTable(const util::Config &query) {
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

  util::Config table_conf(new json(
      {{"name", tmp_table}, {"dimensions", dimensions}, {"metrics", metrics}}));
  controller_.db().CreateTable(tmp_table, table_conf);

  return tmp_table;
}

util::Config Aggregator::CreateWorkerQuery(const util::Config &agg_query) {
  util::Config worker_query = agg_query;
  worker_query.erase("header");
  worker_query.erase("having");
  worker_query.erase("sort");
  worker_query.erase("skip");
  worker_query.erase("limit");
  return worker_query;
}

void Aggregator::RunAggQuery(
    const std::vector<std::vector<std::string>> &target_workers,
    const util::Config &agg_query) {

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

void Aggregator::RunSearchQuery(
    const std::vector<std::vector<std::string>> &target_workers,
    const util::Config &search_query) {

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

void Aggregator::Visit(const RemoteQuery *remote_query) {
  auto &target_workers = remote_query->target_workers();

  if (target_workers.empty()) {
    throw std::runtime_error("query must contain clustering key in its filter");
  }

  if (target_workers.size() == 1) {
    redirect_worker_ =
        target_workers[0][std::rand() % target_workers[0].size()];
    return;
  }

  auto orig_query = remote_query->query();
  auto query_type = orig_query.str("type");
  if (query_type == "aggregate") {
    RunAggQuery(target_workers, orig_query);
  } else if (query_type == "search") {
    RunSearchQuery(target_workers, orig_query);
  } else {
    throw std::runtime_error("unsupported query type");
  }
}

void Aggregator::ShowWorkers() {
  query::RowOutput::Row workers;
  // TODO: implement me
  output_.Start();
  output_.SendAsCol(workers);
  output_.Flush();
}

void Aggregator::Visit(const LocalQuery *local_query) {
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
