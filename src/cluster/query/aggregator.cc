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

#include <random>
#include <evhttp.h>
#include <glog/logging.h>
#include <json.hpp>
#include "db/table.h"
#include "query/query.h"
#include "query/output.h"
#include "input/buffer_loader.h"
#include "util/config.h"
#include "util/scope_guard.h"
#include "cluster/controller.h"
#include "cluster/query/query.h"
#include "cluster/query/aggregator.h"

namespace viya {
namespace cluster {
namespace query {

using json = nlohmann::json;

namespace input = viya::input;

static void on_request_completed(evhttp_request *req, void *obj) {
  static_cast<MultiHttpClient*>(obj)->OnRequestCompleted(req);
}

MultiHttpClient::MultiHttpClient(const std::function<void(const char*, size_t)> response_handler):
  response_handler_(response_handler),requests_(0) {
  event_base_ = event_base_new();
}

MultiHttpClient::~MultiHttpClient() {
  for (auto conn : connections_) {
    evhttp_connection_free(conn);
  }
  if (event_base_) {
    event_base_free(event_base_);
  }
}

void MultiHttpClient::OnRequestCompleted(evhttp_request* request) {
  if (request->response_code != 200) {
    throw std::runtime_error(
      request->response_code_line ? request->response_code_line :
      "wrong HTTP status (" + std::to_string(request->response_code) + ")");
  }

  auto buf_size = EVBUFFER_LENGTH(request->input_buffer);
  auto buf = (char*) EVBUFFER_DATA(request->input_buffer);

  response_handler_(buf, buf_size);

  if (--requests_ == 0) {
    event_base_loopbreak(event_base_);
  }
}

void MultiHttpClient::Send(const std::string& host, uint16_t port, const std::string& path,
                           const std::string& data) {

  auto request = evhttp_request_new(on_request_completed, this);

  evhttp_add_header(request->output_headers, "Content-Type", "application/json");
  evbuffer_add(request->output_buffer, data.c_str(), data.size());

  auto connection = evhttp_connection_base_new(event_base_, nullptr, host.c_str(), port);
  connections_.push_back(connection);

  int ret = evhttp_make_request(connection, request, EVHTTP_REQ_POST, path.c_str());
  if (ret != 0) {
    throw std::runtime_error("evhttp_make_request failed");
  }
  ++requests_;
}

void MultiHttpClient::Await() {
  event_base_dispatch(event_base_);
}

Aggregator::Aggregator(Controller& controller)
  :controller_(controller) {
}

const std::string& Aggregator::SelectWorker(const std::vector<std::string>& workers) {
  return workers[std::rand() % workers.size()];
}

std::string Aggregator::CreateTempTable(const util::Config& query) {
  std::string tmp_table = "query-" + std::to_string(std::rand());

  // Create table descriptor based on the query and the original table:
  query::QueryFactory query_factory;
  std::unique_ptr<query::AggregateQuery> table_query(
    reinterpret_cast<query::AggregateQuery*>(query_factory.Create(query, controller_.db())));

  json* orig_conf = static_cast<json*>(controller_.tables_configs().at(query.str("table")).json_ptr());
  json dimensions = json::array();
  for (auto& it : (*orig_conf)["dimensions"]) {
    auto dim_name = it["name"];
    auto& query_dims = table_query->dimension_cols();
    if (std::find_if(query_dims.begin(), query_dims.end(), [&dim_name](auto& col) {
      return col.dim()->name() == dim_name;
    }) != query_dims.end()) {
      dimensions.push_back(it);
    }
  }

  json metrics = json::array();
  for (auto& it : (*orig_conf)["metrics"]) {
    auto metric_name = it["name"];
    auto& query_metrics = table_query->metric_cols();
    if (std::find_if(query_metrics.begin(), query_metrics.end(), [&metric_name](auto& col) {
      return col.metric()->name() == metric_name;
    }) != query_metrics.end()) {
      metrics.push_back(it);
    }
  }

  util::Config table_conf(new json({{"name", tmp_table}, {"dimensions", dimensions}, {"metrics", metrics}}));
  controller_.db().CreateTable(tmp_table, table_conf);

  return tmp_table;
}

util::Config Aggregator::CreateWorkerQuery(const ClusterQuery& cluster_query) {
  util::Config worker_query = cluster_query.query();
  worker_query.erase("header");
  worker_query.erase("having");
  worker_query.erase("sort");
  worker_query.erase("skip");
  worker_query.erase("limit");
  return worker_query;
}

void Aggregator::RunQuery(const ClusterQuery& cluster_query, query::RowOutput& output) {
  auto worker_query = CreateWorkerQuery(cluster_query);

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

  MultiHttpClient http_client([&load_desc, table](const char* buf, size_t buf_size) {
    input::BufferLoader loader(load_desc, *table, buf, buf_size);
    loader.LoadData();
  });

  auto query_data = worker_query.dump();
  for (auto& replicas : cluster_query.target_workers()) {
    auto& worker_id = SelectWorker(replicas);
    auto sep = worker_id.find(":");
    auto host = worker_id.substr(0, sep);
    auto port = worker_id.substr(sep+1);
    http_client.Send(host, std::atoi(port.c_str()), "/query", query_data);
  }
  http_client.Await();

  util::Config agg_query = cluster_query.query();
  agg_query.set_str("table", tmp_table.c_str());
  agg_query.erase("filter");
  controller_.db().Query(agg_query, output);
}

}}}

