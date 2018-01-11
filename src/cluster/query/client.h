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

#ifndef VIYA_CLUSTER_QUERY_CLIENT_H_
#define VIYA_CLUSTER_QUERY_CLIENT_H_

#include <functional>
#include <unordered_map>
#include <vector>

struct event_base;
struct evhttp_request;
struct evhttp_connection;

namespace viya {
namespace cluster {
namespace query {

class WorkersStates;

class WorkersToTry {
public:
  WorkersToTry(const std::vector<std::string> &workers, size_t current = 0L)
      : workers_(workers), current_(current) {}

  WorkersToTry(const WorkersToTry &other) = delete;

  const std::string &Next();

private:
  const std::vector<std::string> workers_;
  size_t current_;
};

class WorkersClient {
public:
  WorkersClient(
      WorkersStates &workers_states,
      const std::function<void(const char *, size_t)> response_handler);

  WorkersClient(const WorkersClient &) = delete;
  ~WorkersClient();

  void Send(const std::vector<std::string> &workers, const std::string &uri,
            const std::string &data);

  void OnRequestCompleted(evhttp_request *req);
  void Await();

private:
  void Send(WorkersToTry *workers_to_try, const char *uri, const char *data,
            size_t data_size);

private:
  WorkersStates &workers_states_;
  const std::function<void(const char *, size_t)> response_handler_;
  int requests_;
  event_base *event_base_;
  std::vector<evhttp_connection *> connections_;
  std::unordered_map<const evhttp_request *, WorkersToTry *> requests_workers_;
};

} // query namespace
} // cluster namespace
} // viya namespace

#endif // VIYA_CLUSTER_QUERY_CLIENT_H_
