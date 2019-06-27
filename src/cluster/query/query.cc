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

#include "cluster/query/query.h"
#include "cluster/controller.h"
#include "cluster/partitioning.h"
#include "cluster/plan.h"
#include "cluster/query/filter_analyzer.h"
#include "query/filter.h"
#include <algorithm>
#include <boost/functional/hash.hpp>
#include <unordered_set>

namespace viya {
namespace cluster {
namespace query {

namespace query = viya::query;
namespace util = viya::util;

RemoteQuery::RemoteQuery(const util::Config &query,
                         const Controller &controller)
    : RemoteQuery(query,
                  controller.tables_partitioning().at(query.str("table")),
                  controller.tables_plans().at(query.str("table"))) {}

RemoteQuery::RemoteQuery(const util::Config &query,
                         const Partitioning &partitioning, const Plan &plan)
    : ClusterQuery(query), partitioning_(partitioning), plan_(plan) {
  FindTargetWorkers();
}

void RemoteQuery::FindTargetWorkers() {
  target_workers_.clear();

  if (query_.exists("filter")) {
    query::FilterFactory filter_factory;
    std::unique_ptr<query::Filter> filter(
        filter_factory.Create(query_.sub("filter")));

    std::vector<std::string> some_strings;

    struct Hash {
      size_t operator()(const std::vector<std::string> &v) const {
        return boost::hash_range(v.begin(), v.end());
      }
    };
    std::unordered_set<std::vector<std::string>, Hash> workers;

    FilterAnalyzer filter_analyzer(*filter, partitioning_);
    for (auto &partition : filter_analyzer.FindTargetPartitions()) {
      workers.insert(plan_.partitions_workers()[partition]);
    }
    std::copy(workers.begin(), workers.end(),
              std::back_inserter(target_workers_));
  }
}

std::string RemoteQuery::GetRedirectWorker() const {
  if (target_workers_.size() == 1) {
    return target_workers_[0][std::rand() % target_workers_[0].size()];
  }
  return std::string();
}

void RemoteQuery::Accept(ClusterQueryVisitor &visitor) const {
  visitor.Visit(this);
}

void LocalQuery::Accept(ClusterQueryVisitor &visitor) const {
  visitor.Visit(this);
}

void LoadQuery::Accept(ClusterQueryVisitor &visitor) const {
  visitor.Visit(this);
}

std::unique_ptr<ClusterQuery>
ClusterQueryFactory::Create(const util::Config &query,
                            const Controller &controller) {
  auto type = query.str("type");
  if (type == "show") {
    return std::make_unique<LocalQuery>(query);
  }
  if (type == "load") {
    return std::make_unique<LoadQuery>(query);
  }
  return std::make_unique<RemoteQuery>(query, controller);
}

} // namespace query
} // namespace cluster
} // namespace viya
