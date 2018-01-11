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

#include "util/statsd.h"
#include "util/hostname.h"
#include <algorithm>
#include <boost/algorithm/string/replace.hpp>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <glog/logging.h>
#include <iomanip>
#include <netdb.h>
#include <sstream>
#include <sys/socket.h>
#include <sys/types.h>

namespace viya {
namespace util {

Statsd::Statsd() : socket_(-1) {}

void Statsd::Connect(const Config &config) {
  std::string host = config.str("host");
  int port = config.num("port", 8125);
  prefix_ = config.str("prefix", "viyadb.%h.");

  boost::replace_all(prefix_, "%h", get_hostname());

  struct hostent *server = gethostbyname(host.c_str());
  if (server == nullptr) {
    throw std::runtime_error("Unable to resolve host name: " + host);
  }

  memset(&server_, 0, sizeof(server_));
  server_.sin_family = AF_INET;
  memcpy(&server_.sin_addr.s_addr, server->h_addr, server->h_length);
  server_.sin_port = htons(port);

  if ((socket_ = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)) == -1) {
    throw std::runtime_error(std::strerror(errno));
  }

  srandom(static_cast<unsigned int>(time(nullptr)));
}

Statsd::~Statsd() {
  if (socket_ != -1) {
    close(socket_);
  }
}

void Statsd::Send(const std::string &key, int64_t value, float rate,
                  const std::string &unit) const {
  if (socket_ == -1) {
    return;
  }

  if (rate != 1.0 && rate > static_cast<float>(random() / RAND_MAX)) {
    return;
  }

  std::string normalized_key = key;
  std::replace_if(normalized_key.begin(), normalized_key.end(),
                  [](char &ch) { return ch == ':' || ch == '|' || ch == '@'; },
                  '.');

  std::ostringstream buf;
  buf << prefix_ << normalized_key << ":" << value << "|" << unit;
  if (rate != 1.0) {
    buf << "|@" << std::setprecision(1) << rate;
  }
  std::string message = buf.str();

  if (sendto(socket_, message.c_str(), message.length(), 0,
             (struct sockaddr *)&server_, sizeof(server_)) == -1) {
    LOG(ERROR) << "Statsd client unable to send statistics: "
               << std::strerror(errno);
  }
}

void Statsd::Timing(const std::string &key, int64_t value, float rate) const {
  Send(key, value, rate, "ms");
}

void Statsd::Increment(const std::string &key, float rate) const {
  Count(key, 1, rate);
}

void Statsd::Decrement(const std::string &key, float rate) const {
  Count(key, -1, rate);
}

void Statsd::Count(const std::string &key, int64_t value, float rate) const {
  Send(key, value, rate, "c");
}

void Statsd::Gauge(const std::string &key, int64_t value, float rate) const {
  Send(key, value, rate, "g");
}

void Statsd::Set(const std::string &key, int64_t value, float rate) const {
  Send(key, value, rate, "s");
}
}
}
