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

Aggregator::Aggregator(Controller &controller, query::RowOutput &output)
    : controller_(controller), output_(output) {}

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
  for (auto &it : (*orig_conf)["dimensions"]) {
    auto dim_name = it["name"];
    auto &query_dims = table_query->dimension_cols();
    if (std::find_if(query_dims.begin(), query_dims.end(),
                     [&dim_name](auto &col) {
                       return col.dim()->name() == dim_name;
                     }) != query_dims.end()) {
      dimensions.push_back(it);
    }
  }

  json metrics = json::array();
  for (auto &it : (*orig_conf)["metrics"]) {
    auto metric_name = it["name"];
    auto &query_metrics = table_query->metric_cols();
    if (std::find_if(query_metrics.begin(), query_metrics.end(),
                     [&metric_name](auto &col) {
                       return col.metric()->name() == metric_name;
                     }) != query_metrics.end()) {
      metrics.push_back(it);
    }
  }

  util::Config table_conf(new json(
      {{"name", tmp_table}, {"dimensions", dimensions}, {"metrics", metrics}}));
  controller_.db().CreateTable(tmp_table, table_conf);

  return tmp_table;
}

util::Config Aggregator::CreateWorkerQuery(const RemoteQuery *remote_query) {
  util::Config worker_query = remote_query->query();
  worker_query.erase("header");
  worker_query.erase("having");
  worker_query.erase("sort");
  worker_query.erase("skip");
  worker_query.erase("limit");
  return worker_query;
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

  auto worker_query = CreateWorkerQuery(remote_query);

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
      [&load_desc, table](const char *buf, size_t buf_size) {
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

  util::Config agg_query = remote_query->query();
  agg_query.set_str("table", tmp_table);
  agg_query.erase("filter");
  controller_.db().Query(agg_query, output_);
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
}
}
}
