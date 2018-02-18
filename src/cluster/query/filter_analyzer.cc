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

#include "cluster/query/filter_analyzer.h"
#include "util/crc32.h"
#include <boost/algorithm/string/join.hpp>

namespace viya {
namespace cluster {
namespace query {

FilterAnalyzer::FilterAnalyzer(const query::Filter &filter,
                               const Partitioning &partitioning)
    : filter_(filter), partitioning_(partitioning) {}

std::vector<uint32_t> FilterAnalyzer::FindTargetPartitions() {
  filter_.Accept(*this);

  auto &filter_values = stack_.top();
  auto &key_cols = partitioning_.columns();
  std::vector<uint32_t> partitions;

  // Validate that all cluster key columns are provided:
  for (auto &col_values : filter_values) {
    if (col_values.size() != key_cols.size()) {
      std::vector<std::string> missing_cols;
      for (auto &col : key_cols) {
        if (col_values.find(col) == col_values.end()) {
          missing_cols.push_back(col);
        }
      }
      throw std::runtime_error("Key columns are missing from filter: " +
                               boost::algorithm::join(missing_cols, ", "));
    }

    // Calculate the hash code for the key's columns values:
    uint32_t code = 0;
    for (auto &col : key_cols) {
      code = util::crc32(code, col_values.at(col));
    }
    code = code % partitioning_.total();
    // Add target partition number to the result list:
    partitions.push_back(partitioning_.mapping()[code]);
  }

  return partitions;
}

void FilterAnalyzer::Visit(const query::RelOpFilter *filter) {
  stack_.emplace(FilterValues{});

  if (filter->op() == query::RelOpFilter::Operator::EQUAL &&
      IsKeyColumn(filter->column())) {
    AddKeyValue(filter->column(), filter->value());
  }
}

void FilterAnalyzer::Visit(const query::InFilter *filter) {
  stack_.emplace(FilterValues{});

  if (filter->equal() && IsKeyColumn(filter->column())) {
    for (auto &value : filter->values()) {
      AddKeyValue(filter->column(), value);
    }
  }
}

void FilterAnalyzer::Visit(const query::CompositeFilter *filter) {
  stack_.emplace(FilterValues{});

  for (auto f : filter->filters()) {
    f->Accept(*this);

    FilterValues filter_values =
        std::move(const_cast<FilterValues &>(stack_.top()));
    stack_.pop();

    // Merge current filter's with all previous filters' values:
    auto &curr_values = stack_.top();
    if (curr_values.empty()) {
      curr_values.swap(filter_values);
    } else {
      if (filter->op() == query::CompositeFilter::Operator::AND) {
        // Calculate all combinations:
        FilterValues combinations;
        if (filter_values.empty()) {
          combinations.swap(curr_values);
        } else {
          for (auto &cv : curr_values) {
            for (auto &fv : filter_values) {
              combinations.emplace_back(ColumnsValues{});
              auto &last = combinations.back();
              last.insert(cv.begin(), cv.end());
              last.insert(fv.begin(), fv.end());
            }
          }
        }
        curr_values.swap(combinations);
      } else { // OR
        // Union values sets:
        curr_values.insert(curr_values.end(), filter_values.begin(),
                           filter_values.end());
      }
    }
  }
}

void FilterAnalyzer::Visit(const query::EmptyFilter *filter
                           __attribute__((unused))) {}

} // namespace query
} // namespace cluster
} // namespace viya
