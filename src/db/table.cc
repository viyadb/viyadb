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

#include "db/table.h"
#include "codegen/db/metadata.h"
#include "db/column.h"
#include "db/database.h"
#include "db/store.h"
#include "util/sanitize.h"
#include <chrono>
#include <stdexcept>

namespace viya {
namespace db {

namespace cg = viya::codegen;
namespace util = viya::util;
namespace cr = std::chrono;

CardinalityGuard::CardinalityGuard(const util::Config &config,
                                   const Dimension *dim, const Table &table)
    : dim_(dim) {
  if (dim->dim_type() == db::Dimension::DimType::NUMERIC) {
    throw std::invalid_argument(
        "Can't define cardinality guard on a numeric dimension");
  }
  for (auto d : config.strlist("dimensions")) {
    dimensions_.push_back(table.dimension(d));
  }
  limit_ = config.num("limit");
}

Table::Table(const util::Config &config, Database &database)
    : database_(database), segment_size_(config.num("segment_size", 1000000L)) {

  name_ = config.str("name");
  util::check_legal_string("Table name", name_);

  size_t dim_idx = 0;
  for (util::Config &dim_config : config.sublist("dimensions")) {
    std::string dim_type = dim_config.str("type", "string");
    if (dim_type == "string") {
      dimensions_.push_back(
          new StrDimension(dim_config, dim_idx++, database.dicts()));
    } else if (dim_type == "boolean") {
      dimensions_.push_back(new BoolDimension(dim_config, dim_idx++));
    } else if (dim_type == "time") {
      dimensions_.push_back(new TimeDimension(dim_config, dim_idx++, false));
    } else if (dim_type == "microtime") {
      dimensions_.push_back(new TimeDimension(dim_config, dim_idx++, true));
    } else {
      dimensions_.push_back(new NumDimension(dim_config, dim_idx++));
    }
  }

  size_t metric_idx = 0;
  for (util::Config &metric_config : config.sublist("metrics")) {
    if (metric_config.str("type") == "bitset") {
      metrics_.push_back(new BitsetMetric(metric_config, metric_idx++));
    } else {
      metrics_.push_back(new ValueMetric(metric_config, metric_idx++));
    }
  }

  for (util::Config &dim_config : config.sublist("dimensions")) {
    if (dim_config.exists("cardinality_guard")) {
      cardinality_guards_.push_back(
          CardinalityGuard(dim_config.sub("cardinality_guard"),
                           dimension(dim_config.str("name")), *this));
    }
  }

  store_ = new SegmentStore(database, *this);

  if (config.exists("watch")) {
    database_.watcher().AddWatch(config.sub("watch"), this);
  }
}

Table::~Table() {
  database_.watcher().RemoveWatch(this);

  for (auto d : dimensions_) {
    delete d;
  }
  for (auto m : metrics_) {
    delete m;
  }
  delete store_;
}

const Column *Table::column(const std::string &name) const {
  for (auto d : dimensions_)
    if (d->name() == name)
      return d;
  for (auto d : metrics_)
    if (d->name() == name)
      return d;
  throw std::invalid_argument("No such column: " + name);
}

const std::vector<const Column *> Table::columns() const {
  std::vector<const Column *> columns;
  columns.reserve(dimensions_.size() + metrics_.size());
  columns.insert(columns.end(), dimensions_.begin(), dimensions_.end());
  columns.insert(columns.end(), metrics_.begin(), metrics_.end());
  return std::move(columns);
}

const Dimension *Table::dimension(const std::string &name) const {
  for (auto d : dimensions_)
    if (d->name() == name)
      return d;
  throw std::invalid_argument("No such dimension: " + name);
}

const Metric *Table::metric(const std::string &name) const {
  for (auto d : metrics_)
    if (d->name() == name)
      return d;
  throw std::invalid_argument("No such metric: " + name);
}

void Table::PrintMetadata(std::string &output) {
  auto table_metadata =
      cg::TableMetadata(database_.compiler(), *this).Function();
  table_metadata(*this, output);
}
}
}
