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

#include "cluster/query/client.h"
#include "cluster/query/worker_state.h"
#include <evhttp.h>

namespace viya {
namespace cluster {
namespace query {

const std::string &WorkersToTry::Next() {
  if (current_ >= workers_.size()) {
    throw std::runtime_error("no more workers to try");
  }
  return workers_[current_++];
}

static void on_connection_closed(evhttp_connection *conn, void *obj) {
  static_cast<WorkersClient *>(obj)->OnConnectionClosed(conn);
}

static void on_request_completed(evhttp_request *req, void *obj) {
  static_cast<WorkersClient *>(obj)->OnRequestCompleted(req);
}

WorkersClient::WorkersClient(
    WorkersStates &workers_states,
    const std::function<void(const char *, size_t)> response_handler)
    : workers_states_(workers_states), response_handler_(response_handler),
      requests_(0) {
  event_base_ = event_base_new();
}

WorkersClient::~WorkersClient() {
  for (auto &it : requests_workers_) {
    delete it.second;
  }
  for (auto conn : connections_) {
    evhttp_connection_free(conn);
  }
  if (event_base_) {
    event_base_free(event_base_);
  }
}

void WorkersClient::OnConnectionClosed(evhttp_connection *conn
                                       __attribute__((unused))) {}

void WorkersClient::OnRequestCompleted(evhttp_request *request) {
  if (!request) {
    // eek -- this is just a clean-up notification because the connection's
    // been closed, but we already dealt with it in onConnectionClosed
    return;
  }

  if (request->response_code != 200) {
    throw std::runtime_error(request->response_code_line
                                 ? request->response_code_line
                                 : "wrong HTTP status (" +
                                       std::to_string(request->response_code) +
                                       ")");
  }

  if (false) { // TODO: decide that we want to query other worker
    auto data = (char *)EVBUFFER_DATA(request->output_buffer);
    auto data_size = EVBUFFER_LENGTH(request->output_buffer);

    Send(requests_workers_.at(request), request->uri, data, data_size);
    requests_workers_.erase(request);

  } else {
    auto buf = (char *)EVBUFFER_DATA(request->input_buffer);
    auto buf_size = EVBUFFER_LENGTH(request->input_buffer);

    response_handler_(buf, buf_size);

    if (--requests_ == 0) {
      event_base_loopbreak(event_base_);
    }
  }
}

void WorkersClient::Send(WorkersToTry *workers_to_try, const char *uri,
                         const char *data, size_t data_size) {
  while (true) {
    auto &worker_id = workers_to_try->Next();
    if (workers_states_.IsFailing(worker_id)) {
      continue;
    }
    auto sep = worker_id.find(":");
    auto host = worker_id.substr(0, sep);
    auto port = worker_id.substr(sep + 1);

    auto request = evhttp_request_new(on_request_completed, this);

    evhttp_add_header(request->output_headers, "Content-Type",
                      "application/json");
    evhttp_add_header(request->output_headers, "Connection", "close");
    evbuffer_add(request->output_buffer, data, data_size);

    auto connection = evhttp_connection_base_new(
        event_base_, nullptr, host.c_str(), std::atoi(port.c_str()));
    evhttp_connection_set_closecb(connection, on_connection_closed, this);
    connections_.push_back(connection);

    if (evhttp_make_request(connection, request, EVHTTP_REQ_POST, uri) == 0) {
      requests_workers_.insert(std::make_pair(request, workers_to_try));
      break;
    }
    workers_states_.SetFailing(worker_id);
  }
}

void WorkersClient::Send(const std::vector<std::string> &workers,
                         const std::string &uri, const std::string &data) {
  Send(new WorkersToTry(workers), uri.c_str(), data.c_str(), data.size());
  ++requests_;
}

void WorkersClient::Await() { event_base_dispatch(event_base_); }

} // namespace query
} // namespace cluster
} // namespace viya
