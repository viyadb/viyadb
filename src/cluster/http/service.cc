/*
 * Copyright (c) 2017 ViyaDB Group
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

#include <algorithm>
#include <glog/logging.h>
#include "sql/driver.h"
#include "cluster/http/service.h"
#include "cluster/controller.h"
#include "cluster/query.h"

namespace viya {
namespace cluster {
namespace http {

namespace sql = viya::sql;

Service::Service(Controller& controller):controller_(controller) {
  server_.config.port = controller.cluster_config().num("http_port", 5555);
  server_.config.reuse_address = true;
}

void Service::SendError(ResponsePtr response, const std::string& error) {
  LOG(ERROR)<<error;
  *response<<"HTTP/1.1 400 Bad Request\r\nContent-Length: "<<error.size()<<"\r\n\r\n"<<error;
}

void Service::ProcessQuery(const util::Config& query, ResponsePtr response, RequestPtr request) {
  ClusterQuery cluster_query(query, controller_);
  auto target_workers = cluster_query.target_workers();
  if (target_workers.size() > 1) {
    throw std::runtime_error("aggregation of results is not supported at the moment");
  }
  if (target_workers.empty()) {
    *response<<"HTTP/1.1 201 OK\r\nContent-Length: 0\r\n\r\n";
  } else {
    std::string worker_url = controller_.WorkerUrl(*target_workers.begin())
      + request->path + (request->query_string.empty() ? "" : "?" + request->query_string);
    *response<<"HTTP/1.1 307 Temporary Redirect\r\n"
      <<"Content-Length: 0\r\nLocation: "<<worker_url<<"\r\n\r\n";
  }
}

void Service::Start() {
  server_.resource["^/query(\\?.*)?$"]["POST"] = [&](ResponsePtr response, RequestPtr request) {
    try {
      util::Config query(request->content.string());
      ProcessQuery(query, response, request);
    } catch (std::exception& e) {
      SendError(response, std::string(e.what()));
    }
  };

  server_.resource["^/sql(\\?.*)?$"]["POST"] = [&](ResponsePtr response, RequestPtr request) {
    try {
      sql::Driver sql_driver(controller_.db());
      std::istringstream is(request->content.string());
      auto queries = std::move(sql_driver.ParseQueries(is));
      if (queries.size() == 1) {
        ProcessQuery(queries[0], response, request);
      } else if (queries.size() > 1) {
        SendError(response, "multiple queries are not supported");
      }
    } catch (std::exception& e) {
      SendError(response, std::string(e.what()));
    }
  };

  LOG(INFO)<<"Started HTTP service on port "<<std::to_string(server_.config.port)<<std::endl;
  server_.start();
}

}}}

