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

#include "query/query.h"
#include "db/database.h"
#include "db/table.h"
#include "query/filter.h"
#include "util/sanitize.h"

namespace viya {
namespace query {

FilterBasedQuery::FilterBasedQuery(const util::Config &config, db::Table &table)
    : TableQuery(table) {
  FilterFactory filter_factory;
  filter_ = filter_factory.Create(config);
}

DimOutputColumn::DimOutputColumn(const util::Config &config,
                                 const db::Dimension *dim, size_t index)
    : OutputColumn(index), dim_(dim) {
  if (dim->dim_type() == db::Dimension::DimType::TIME) {
    format_ = config.str("format", "");
    util::check_legal_string("Time format", format_);
    if (config.exists("granularity")) {
      granularity_ = db::Granularity(config.str("granularity"));
    }
  }
}

AggregateQuery::AggregateQuery(const util::Config &config, db::Table &table)
    : FilterBasedQuery(config.sub("filter", true), table),
      skip_(config.num("skip", 0)), limit_(config.num("limit", 0)),
      header_(config.boolean("header", false)), having_(nullptr) {

  size_t output_idx = 0;

  if (config.exists("select")) {
    auto add_column = [&output_idx, this](util::Config &conf,
                                          const db::Column *column) {
      if (column->type() == db::Column::Type::DIMENSION) {
        dimension_cols_.emplace_back(
            conf, static_cast<const db::Dimension *>(column), output_idx++);
      } else {
        metric_cols_.emplace_back(conf, static_cast<const db::Metric *>(column),
                                  output_idx++);
      }
    };
    for (util::Config &select_conf : config.sublist("select")) {
      auto column_name = select_conf.str("column");
      if (column_name == "*") {
        for (auto column : table.columns()) {
          add_column(select_conf, column);
        }
      } else {
        add_column(select_conf, table.column(column_name));
      }
    }
  } else {
    for (auto dim_name : config.strlist("dimensions")) {
      dimension_cols_.emplace_back(table.dimension(dim_name), output_idx++);
    }
    for (auto metric_name : config.strlist("metrics")) {
      metric_cols_.emplace_back(table.metric(metric_name), output_idx++);
    }
  }

  if (config.exists("having")) {
    FilterFactory filter_factory;
    having_ = filter_factory.Create(config.sub("having"));

    ColumnsCollector columns_collector;
    having_->Accept(columns_collector);
    auto query_cols = column_names();
    for (auto &having_col : columns_collector.columns()) {
      if (std::find(query_cols.begin(), query_cols.end(), having_col) ==
          query_cols.end()) {
        throw std::invalid_argument("Column '" + having_col +
                                    " is not selected");
      }
    }
  }

  if (config.exists("sort")) {
    for (auto &sort_conf : config.sublist("sort")) {
      auto sort_column = sort_conf.str("column");
      auto col = table.column(sort_column);
      bool ascending = sort_conf.boolean("ascending", false);

      // Find index of sort column in the result row:
      int col_idx = -1;
      for (auto &dim_col : dimension_cols_) {
        if (dim_col.dim() == col) {
          col_idx = dim_col.index();
          break;
        }
      }
      if (col_idx == -1) {
        for (auto &metric_col : metric_cols_) {
          if (metric_col.metric() == col) {
            col_idx = metric_col.index();
            break;
          }
        }
      }
      if (col_idx == -1) {
        throw std::invalid_argument("Sort column '" + sort_column +
                                    "' is not selected");
      }
      sort_cols_.push_back(SortColumn(col, col_idx, ascending));
    }
  }
}

void AggregateQuery::Accept(QueryVisitor &visitor) { visitor.Visit(this); }

SearchQuery::SearchQuery(const util::Config &config, db::Table &table)
    : FilterBasedQuery(config.sub("filter", true), table),
      dimension_(table.dimension(config.str("dimension"))),
      term_(config.str("term")), limit_(config.num("limit", 0)) {}

void SearchQuery::Accept(QueryVisitor &visitor) { visitor.Visit(this); }

ShowTablesQuery::ShowTablesQuery(db::Database &db) : DatabaseQuery(db) {}

void ShowTablesQuery::Accept(QueryVisitor &visitor) { visitor.Visit(this); }

Query *QueryFactory::Create(const util::Config &config,
                            db::Database &database) {
  auto type = config.str("type");
  if (type == "aggregate") {
    auto table = database.GetTable(config.str("table"));
    return new AggregateQuery(config, *table);
  }
  if (type == "search") {
    auto table = database.GetTable(config.str("table"));
    return new SearchQuery(config, *table);
  }
  if (type == "show") {
    auto what = config.str("what");
    if (what == "tables") {
      return new ShowTablesQuery(database);
    }
    throw std::invalid_argument("don't know how to show " + what);
  }
  throw std::invalid_argument("unsupported query type: " + type);
}

} // namespace query
} // namespace viya
