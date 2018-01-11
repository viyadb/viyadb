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

#ifndef VIYA_CLUSTER_WORKER_H_
#define VIYA_CLUSTER_WORKER_H_

#include "cluster/consul/consul.h"
#include "util/config.h"
#include <memory>

namespace viya {
namespace cluster {

namespace util = viya::util;

class Worker {
public:
  Worker(const util::Config &config);
  Worker(const Worker &other) = delete;

  void CreateKey() const;

private:
  const std::string id_;
  const util::Config &config_;
  const consul::Consul consul_;
  std::unique_ptr<consul::Session> session_;
};
}
}

#endif // VIYA_CLUSTER_WORKER_H_
