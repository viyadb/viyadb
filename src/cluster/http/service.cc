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

#include "cluster/http/service.h"
#include "cluster/controller.h"
#include "cluster/query/query.h"
#include "server/http/output.h"
#include "sql/driver.h"
#include "sql/statement.h"
#include <algorithm>
#include <boost/exception/diagnostic_information.hpp>
#include <glog/logging.h>

namespace viya {
namespace cluster {
namespace http {

namespace sql = viya::sql;
namespace server = viya::server;

Service::Service(Controller &controller)
    : controller_(controller), query_runner_(controller) {
  server_.config.port = controller.cluster_config().num("http_port", 5555);
  server_.config.reuse_address = true;
  server_.config.thread_pool_size = 4;
}

void Service::SendError(ResponsePtr response, const std::string &error) {
  LOG(ERROR) << error;
  *response << "HTTP/1.1 400 Bad Request\r\nContent-Length: " << error.size()
            << "\r\n\r\n"
            << error;
}

void Service::ProcessQuery(const util::Config &query, ResponsePtr response,
                           RequestPtr request) {

  auto cluster_query = query::ClusterQueryFactory::Create(query, controller_);

  auto redirect_worker = cluster_query->GetRedirectWorker();
  if (!redirect_worker.empty()) {

    std::ostringstream worker_url;
    worker_url << "http://" << redirect_worker << request->path;
    if (!request->query_string.empty()) {
      worker_url << "?" << request->query_string;
    }
    *response << "HTTP/1.1 307 Temporary Redirect\r\n"
              << "Content-Length: 0\r\nLocation: " << worker_url.str()
              << "\r\n\r\n";
  } else {
    server::http::ChunkedTsvOutput output(*response);
    query_runner_.Run(*cluster_query, output);
  }
}

void Service::Start() {
  server_.resource["^/query(\\?.*)?$"]["POST"] = [&](ResponsePtr response,
                                                     RequestPtr request) {
    try {
      util::Config query(request->content.string());
      ProcessQuery(query, response, request);
    } catch (const std::exception &e) {
      SendError(response, e.what());
    } catch (...) {
      SendError(response, boost::current_exception_diagnostic_information());
    }
  };

  server_.resource["^/load$"]["POST"] = [&](ResponsePtr response,
                                            RequestPtr request) {
    try {
      util::Config load_conf(request->content.string());
      controller_.feeder().LoadData(load_conf);
      *response << "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
    } catch (const std::exception &e) {
      SendError(response, e.what());
    } catch (...) {
      SendError(response, boost::current_exception_diagnostic_information());
    }
  };

  server_.resource["^/sql(\\?.*)?$"]["POST"] = [&](ResponsePtr response,
                                                   RequestPtr request) {
    try {
      auto params = request->parse_query_string();
      bool add_header = params.find("header") != params.end();
      sql::Driver sql_driver(controller_.db(), add_header);
      std::istringstream is(request->content.string());

      auto stmts = sql_driver.ParseStatements(is);
      if (stmts.size() == 1) {
        util::Config query(stmts.at(0).descriptor());
        ProcessQuery(query, response, request);
      } else if (stmts.size() > 1) {
        SendError(response, "multiple SQL statements are not supported");
      }
    } catch (const std::exception &e) {
      SendError(response, e.what());
    } catch (...) {
      SendError(response, boost::current_exception_diagnostic_information());
    }
  };

  LOG(INFO) << "Started HTTP service on port "
            << std::to_string(server_.config.port) << std::endl;
  server_.start();
}

} // namespace http
} // namespace cluster
} // namespace viya
