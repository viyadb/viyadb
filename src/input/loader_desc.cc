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

#include <algorithm>
#include "db/column.h"
#include "db/table.h"
#include "util/config.h"
#include "input/loader.h"
#include "input/loader_desc.h"

namespace viya {
namespace input {

namespace db = viya::db;

PartitionFilter::PartitionFilter(const util::Config& config):
  columns_(config.strlist("columns")),
  total_partitions_((size_t)config.num("total_partitions")),
  values_(config.numlist_uint32("values")) {
}

LoaderDesc::LoaderDesc(const util::Config& config, const db::Table& table):
  config_(config),
  table_(table),
  format_(Format::UNKNOWN) {

  if (config.exists("format")) {
    std::string fmt = config.str("format");
    if (fmt == "tsv") {
      format_ = Format::TSV;
    }
  }
  if (config.exists("file")) {
    fname_ = config.str("file");
  }
  if (config.exists("partition_filter")) {
    partition_filter_.reset(new PartitionFilter(config.sub("partition_filter")));
  }

  InitTupleIdxMap();
}

void LoaderDesc::InitTupleIdxMap() {
  auto columns = table_.columns();
  auto has_field_mapping = std::find_if(columns.begin(), columns.end(), [](auto col) {
    return !col->input_field().empty();
  }) != columns.end();

  std::vector<const db::Column*> input_cols;
  for (auto dimension : table_.dimensions()) {
    input_cols.push_back(dimension);
  }
  for (auto metric : table_.metrics()) {
    if (metric->agg_type() != db::Metric::AggregationType::COUNT) {
      input_cols.push_back(metric);
    }
  }
  tuple_idx_map_.resize(input_cols.size());

  if (config_.exists("columns")) {
    auto load_cols = config_.strlist("columns");
    size_t idx = 0;
    for (auto col : input_cols) {
      const std::string& col_name = col->input_field().empty() ? col->name() : col->input_field();
      auto it = std::find(load_cols.begin(), load_cols.end(), col_name);
      if (it == load_cols.end()) {
        throw std::runtime_error("Column name '" + col_name + "' is not specified in load spec");
      }
      tuple_idx_map_[idx++] = std::distance(load_cols.begin(), it);
    }
  } else {
    if (has_field_mapping) {
      throw std::runtime_error(
        "Column names must be specified, because one or more columns define field name mapping");
    }
    for (size_t idx = 0; idx < input_cols.size(); ++idx) {
      tuple_idx_map_[idx] = idx;
    }
  }
}

}}
