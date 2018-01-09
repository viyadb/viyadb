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

#include <stack>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include <boost/functional/hash.hpp>
#include <glog/logging.h>
#include "query/filter.h"
#include "util/crc32.h"
#include "cluster/controller.h"
#include "cluster/partitioning.h"
#include "cluster/plan.h"
#include "cluster/query/query.h"
 
namespace viya {
namespace cluster {
namespace query {

namespace query = viya::query;

class FilterAnalyzer : public query::FilterVisitor {
  using ColumnsValues = std::unordered_map<std::string, std::string>;
  using FilterValues = std::vector<ColumnsValues>;

  public:
    FilterAnalyzer(const query::Filter& filter, const Partitioning& partitioning);

    std::vector<uint32_t> FindTargetPartitions();

    void Visit(const query::RelOpFilter* filter);
    void Visit(const query::InFilter* filter);
    void Visit(const query::CompositeFilter* filter);
    void Visit(const query::EmptyFilter* filter);

  private:
    bool IsKeyColumn(const std::string& col) {
      return std::find_if(partitioning_.columns().begin(),
                          partitioning_.columns().end(),
                          [&col](auto& kc) { return col == kc; }) != partitioning_.columns().end();
    }

    void AddKeyValue(const std::string& col, const std::string& value) {
      auto& values = stack_.top();
      values.emplace_back(ColumnsValues {});
      values.back().emplace(col, value);
    }

  private:
    std::stack<FilterValues> stack_;
    const query::Filter& filter_;
    const Partitioning& partitioning_;
};

FilterAnalyzer::FilterAnalyzer(const query::Filter& filter, const Partitioning& partitioning):
  filter_(filter),partitioning_(partitioning) {
}

std::vector<uint32_t> FilterAnalyzer::FindTargetPartitions() {
  filter_.Accept(*this);

  auto& filter_values = stack_.top();
  auto& key_cols = partitioning_.columns();
  std::vector<uint32_t> partitions;

  // Validate that all cluster key columns are provided:
  for (auto& col_values : filter_values) {
    if (col_values.size() != key_cols.size()) {
      std::vector<std::string> missing_cols;
      for (auto& col: key_cols) {
        if (col_values.find(col) == col_values.end()) {
          missing_cols.push_back(col);
        }
      }
      throw std::runtime_error(
        "Key columns are missing from filter: " + boost::algorithm::join(missing_cols, ", "));
    }

    // Calculate the hash code for the key's columns values:
    uint32_t code = 0;
    for (auto& col : key_cols) {
      code = crc32(code, col_values.at(col));
    }
    code = code % partitioning_.total();
    // Add target partition number to the result list:
    partitions.push_back(partitioning_.mapping()[code]);
  }

  return partitions;
}

void FilterAnalyzer::Visit(const query::RelOpFilter* filter) {
  stack_.emplace(FilterValues {});

  if (filter->op() == query::RelOpFilter::Operator::EQUAL && IsKeyColumn(filter->column())) {
    AddKeyValue(filter->column(), filter->value());
  }
}

void FilterAnalyzer::Visit(const query::InFilter* filter) {
  stack_.emplace(FilterValues {});

  if (filter->equal() && IsKeyColumn(filter->column())) {
    for (auto& value : filter->values()) {
      AddKeyValue(filter->column(), value);
    }
  }
}

void FilterAnalyzer::Visit(const query::CompositeFilter* filter) {
  stack_.emplace(FilterValues {});

  for (auto f : filter->filters()) {
    f->Accept(*this);

    FilterValues filter_values = std::move(const_cast<FilterValues&>(stack_.top()));
    stack_.pop();

    // Merge current filter's with all previous filters' values:
    auto& curr_values = stack_.top();
    if (curr_values.empty()) {
      curr_values.swap(filter_values);
    } else {
      if (filter->op() == query::CompositeFilter::Operator::AND) {
        // Calculate all combinations:
        FilterValues combinations;
        if (filter_values.empty()) {
          combinations.swap(curr_values);
        } else {
          for (auto& cv : curr_values) {
            for (auto& fv : filter_values) {
              combinations.emplace_back(ColumnsValues {});
              auto& last = combinations.back();
              last.insert(cv.begin(), cv.end());
              last.insert(fv.begin(), fv.end());
            }
          }
        }
        curr_values.swap(combinations);
      } else { // OR
        // Union values sets:
        curr_values.insert(curr_values.end(), filter_values.begin(), filter_values.end());
      }
    }
  }
}

void FilterAnalyzer::Visit(const query::EmptyFilter* filter __attribute__((unused))) {
}

RemoteQuery::RemoteQuery(const util::Config& query, const Controller& controller):
  RemoteQuery(
    query,
    controller.tables_partitioning().at(query.str("table")),
    controller.tables_plans().at(query.str("table"))) {
}

RemoteQuery::RemoteQuery(const util::Config& query, const Partitioning& partitioning, const Plan& plan):
  ClusterQuery(query),partitioning_(partitioning),plan_(plan) {

  FindTargetWorkers();
}

void RemoteQuery::FindTargetWorkers() {
  target_workers_.clear();

  if (query_.exists("filter")) {
    query::FilterFactory filter_factory;
    std::unique_ptr<query::Filter> filter(filter_factory.Create(query_.sub("filter")));

    std::vector<std::string> some_strings;

    struct Hash {
      size_t operator() (const std::vector<std::string>& v) const {
        return boost::hash_range(v.begin(), v.end());
      }
    };
    std::unordered_set<std::vector<std::string>, Hash> workers;

    FilterAnalyzer filter_analyzer(*filter, partitioning_);
    for (auto& partition : filter_analyzer.FindTargetPartitions()) {
      workers.insert(plan_.partitions_workers()[partition]);
    }
    std::copy(workers.begin(), workers.end(), std::back_inserter(target_workers_));
  }
}

void RemoteQuery::Accept(ClusterQueryVisitor& visitor) {
  visitor.Visit(this);
}

void LocalQuery::Accept(ClusterQueryVisitor& visitor) {
  visitor.Visit(this);
}

std::unique_ptr<ClusterQuery> ClusterQueryFactory::Create(
  const util::Config& query, const Controller& controller) {
  if (query.str("type") == "show") {
    return std::move(std::make_unique<LocalQuery>(query));
  }
  return std::move(std::make_unique<RemoteQuery>(query, controller));
}

}}}

