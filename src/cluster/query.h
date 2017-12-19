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

#ifndef VIYA_CLUSTER_QUERY_H_
#define VIYA_CLUSTER_QUERY_H_

#include <memory>
#include "util/config.h"

namespace viya {
namespace cluster {

class Controller;

namespace util = viya::util;

class ClusterQuery {
  public:
    ClusterQuery(const Controller& controller, const util::Config& query);

  private:
    void BuildRemoteQueries();

  private:
    const Controller& controller_;
    const util::Config& query_;
    std::unordered_map<std::string, util::Config> remote_queries_;
};

}}

#endif // VIYA_CLUSTER_QUERY_H_
