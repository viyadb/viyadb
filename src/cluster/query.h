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
#include <unordered_set>
#include "util/config.h"

namespace viya {
namespace cluster {

class Controller;
class Partitioning;
class Plan;

namespace util = viya::util;

class ClusterQuery {
  public:
    ClusterQuery(const util::Config& query, const Controller& controller);
    ClusterQuery(const util::Config& query, const Partitioning& partitioning, const Plan& plan);

    const std::unordered_set<std::string>& target_workers() const { return target_workers_; }

  private:
    void FindTargetWorkers();

  private:
    const util::Config& query_;
    const Partitioning& partitioning_;
    const Plan& plan_;
    std::unordered_set<std::string> target_workers_;
};

}}

#endif // VIYA_CLUSTER_QUERY_H_
