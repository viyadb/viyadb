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

#ifndef VIYA_CLUSTER_QUERY_AGGREGATOR_H_
#define VIYA_CLUSTER_QUERY_AGGREGATOR_H_

#include <vector>
#include <functional>
#include "util/config.h"

struct event_base;
struct evhttp_request;
struct evhttp_connection;

namespace viya { namespace cluster { class Controller; }}
namespace viya { namespace query { class RowOutput; }}

namespace viya {
namespace cluster {
namespace query {

class ClusterQuery;

namespace util = viya::util;
namespace query = viya::query;

class MultiHttpClient {
  public:
    MultiHttpClient(const std::function<void(const char*, size_t)> response_handler);
    MultiHttpClient(const MultiHttpClient&) = delete;
    ~MultiHttpClient();

    void Send(const std::string& host, uint16_t port, const std::string& path,
              const std::string& data);

    void Await();

    void OnRequestCompleted(evhttp_request* req);

  private:
    const std::function<void(const char*, size_t)> response_handler_;
    int requests_;
    event_base* event_base_;
    std::vector<evhttp_connection*> connections_;
};

class Aggregator {
  public:
    Aggregator(Controller& controller);

    void RunQuery(const ClusterQuery& cluster_query, query::RowOutput& output);

  protected:
    std::string CreateTempTable(const util::Config& worker_query);
    util::Config CreateWorkerQuery(const ClusterQuery& cluster_query);
    const std::string& SelectWorker(const std::vector<std::string>& workers);

  private:
    Controller& controller_;
};

}}}

#endif // VIYA_CLUSTER_QUERY_AGGREGATOR_H_
