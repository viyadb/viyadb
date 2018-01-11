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

#ifndef VIYA_UTIL_STATSD_H_
#define VIYA_UTIL_STATSD_H_

#include "util/config.h"
#include <netinet/in.h>

namespace viya {
namespace util {

class Statsd {
public:
  Statsd();
  Statsd(Statsd &other) = delete;
  ~Statsd();

  void Connect(const Config &config);
  void Timing(const std::string &key, int64_t value, float rate = 1.0) const;
  void Increment(const std::string &key, float rate = 1.0) const;
  void Decrement(const std::string &key, float rate = 1.0) const;
  void Count(const std::string &key, int64_t value, float rate = 1.0) const;
  void Gauge(const std::string &key, int64_t value, float rate = 1.0) const;
  void Set(const std::string &key, int64_t value, float rate = 1.0) const;

protected:
  void Send(const std::string &key, int64_t value, float rate,
            const std::string &unit) const;

private:
  std::string prefix_;
  struct sockaddr_in server_;
  int socket_;
};
}
}

#endif // VIYA_UTIL_STATSD_H_
