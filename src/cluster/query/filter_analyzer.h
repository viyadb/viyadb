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

#ifndef VIYA_CLUSTER_QUERY_FILTER_ANALYZER_H_
#define VIYA_CLUSTER_QUERY_FILTER_ANALYZER_H_

#include "cluster/partitioning.h"
#include "query/filter.h"
#include <algorithm>
#include <stack>
#include <unordered_map>
#include <vector>

namespace viya {
namespace cluster {
namespace query {

namespace query = viya::query;
namespace util = viya::util;

class FilterAnalyzer : public query::FilterVisitor {
  using ColumnsValues = std::unordered_map<std::string, std::string>;
  using FilterValues = std::vector<ColumnsValues>;

public:
  FilterAnalyzer(const query::Filter &filter, const Partitioning &partitioning);

  std::vector<uint32_t> FindTargetPartitions();

  void Visit(const query::RelOpFilter *filter);
  void Visit(const query::InFilter *filter);
  void Visit(const query::CompositeFilter *filter);
  void Visit(const query::EmptyFilter *filter);

private:
  bool IsKeyColumn(const std::string &col) {
    return std::find(partitioning_.columns().begin(),
                     partitioning_.columns().end(),
                     col) != partitioning_.columns().end();
  }

  void AddKeyValue(const std::string &col, const std::string &value) {
    auto &values = stack_.top();
    values.emplace_back(ColumnsValues{});
    values.back().emplace(col, value);
  }

private:
  std::stack<FilterValues> stack_;
  const query::Filter &filter_;
  const Partitioning &partitioning_;
};

} // namespace query
} // namespace cluster
} // namespace viya

#endif // VIYA_CLUSTER_QUERY_FILTER_ANALYZER_H_
