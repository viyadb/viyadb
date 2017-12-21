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

#include <stack>
#include <set>
#include <unordered_map>
#include <algorithm>
#include <boost/algorithm/string/join.hpp>
#include "query/filter.h"
#include "util/crc32.h"
#include "cluster/controller.h"
#include "cluster/partitioning.h"
#include "cluster/plan.h"
#include "cluster/query.h"
 
namespace viya {
namespace cluster {

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
  stack_.emplace(FilterAnalyzer::FilterValues {});
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
  stack_.emplace(FilterAnalyzer::FilterValues {});

  if (filter->op() == query::RelOpFilter::Operator::EQUAL && IsKeyColumn(filter->column())) {
    AddKeyValue(filter->column(), filter->value());
  }
}

void FilterAnalyzer::Visit(const query::InFilter* filter) {
  stack_.emplace(FilterAnalyzer::FilterValues {});

  if (IsKeyColumn(filter->column())) {
    for (auto& value : filter->values()) {
      AddKeyValue(filter->column(), value);
    }
  }
}

void FilterAnalyzer::Visit(const query::CompositeFilter* filter) {
  stack_.emplace(FilterAnalyzer::FilterValues {});
  for (auto f : filter->filters()) {
    f->Accept(*this);
  }

  FilterAnalyzer::FilterValues lh = std::move(const_cast<FilterAnalyzer::FilterValues&>(stack_.top()));
  stack_.pop();

  FilterAnalyzer::FilterValues rh = std::move(const_cast<FilterAnalyzer::FilterValues&>(stack_.top()));
  stack_.pop();

  // Merge `rh` into `lh`:
  auto& curr_values = stack_.top();
  if (filter->op() == query::CompositeFilter::Operator::AND) {
    // Calculate all combinations:
    for (auto& lh_vals : lh) {
      for (auto& rh_vals : rh) {
        curr_values.emplace_back(ColumnsValues {});
        auto& col_vals = curr_values.back();
        col_vals.insert(lh_vals.begin(), lh_vals.end());
        col_vals.insert(rh_vals.begin(), rh_vals.end());
      }
    }
  } else { // OR
    // Union values sets:
    lh.insert(lh.end(), rh.begin(), rh.end());
    curr_values.swap(lh);
  }
}

void FilterAnalyzer::Visit(const query::EmptyFilter* filter __attribute__((unused))) {
}

ClusterQuery::ClusterQuery(const util::Config& query, const Controller& controller):
  ClusterQuery(
    query,
    controller.tables_partitioning().at(query.str("table")),
    controller.tables_plans().at(query.str("table"))) {
}

ClusterQuery::ClusterQuery(const util::Config& query, const Partitioning& partitioning, const Plan& plan):
  query_(query),partitioning_(partitioning),plan_(plan) {

  FindTargetWorkers();
}

void ClusterQuery::FindTargetWorkers() {
  target_workers_.clear();

  query::FilterFactory filter_factory;
  std::unique_ptr<query::Filter> filter(filter_factory.Create(query_.sub("filter")));

  FilterAnalyzer filter_analyzer(*filter, partitioning_);
  for (auto& partition : filter_analyzer.FindTargetPartitions()) {
    for (auto& worker_id : plan_.partitions_workers()[partition]) {
      target_workers_.insert(worker_id);
    }
  }
}

}}

