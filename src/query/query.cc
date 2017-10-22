#include "db/table.h"
#include "db/database.h"
#include "query/filter.h"
#include "query/query.h"
#include "util/sanitize.h"
 
namespace viya {
namespace query {

FilterBasedQuery::FilterBasedQuery(const util::Config& config, db::Table& table)
  :Query(table)
{
  FilterFactory filter_factory;
  filter_ = filter_factory.Create(config, table);
}

DimOutputColumn::DimOutputColumn(const util::Config& config, const db::Dimension* dim, size_t index)
  :OutputColumn(index),dim_(dim)
{
  if (dim->dim_type() == db::Dimension::DimType::TIME) {
    format_ = config.str("format", "");
    util::check_legal_string("Time format", format_);
    if (config.exists("granularity")) {
      granularity_ = db::Granularity(config.str("granularity"));
    } 
  }
}

AggregateQuery::AggregateQuery(const util::Config& config, db::Table& table)
  :FilterBasedQuery(config.sub("filter"), table),
  skip_(config.num("skip", 0)),
  limit_(config.num("limit", 0)),
  header_(config.boolean("header", false)),
  having_(nullptr) {

  size_t output_idx = 0;
  if (config.exists("select")) {
    for (util::Config& select_conf : config.sublist("select")) {
      const auto column = table.column(select_conf.str("column"));
      if (column->type() == db::Column::Type::DIMENSION) {
        dimension_cols_.emplace_back(select_conf, static_cast<const db::Dimension*>(column), output_idx++);
      } else {
        metric_cols_.emplace_back(select_conf, static_cast<const db::Metric*>(column), output_idx++);
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
    having_ = filter_factory.Create(config.sub("having"), table);
  }

  if (config.exists("sort")) {
    for (auto& sort_conf : config.sublist("sort")) {
      auto sort_column = sort_conf.str("column");
      auto col = table.column(sort_column);
      bool ascending = sort_conf.boolean("ascending", false);

      // Find index of sort column in the result row:
      int col_idx = -1;
      for (auto& dim_col : dimension_cols_) {
        if (dim_col.dim() == col) {
          col_idx = dim_col.index();
          break;
        }
      }
      if (col_idx == -1) {
        for (auto& metric_col : metric_cols_) {
          if (metric_col.metric() == col) {
            col_idx = metric_col.index();
            break;
          }
        }
      }
      if (col_idx == -1) {
        throw std::invalid_argument("Sort column '" + sort_column + "' is not selected");
      }
      sort_cols_.push_back(SortColumn(col, col_idx, ascending));
    }
  }
}

void AggregateQuery::Accept(QueryVisitor& visitor) {
  visitor.Visit(this);
}

SearchQuery::SearchQuery(const util::Config& config, db::Table& table)
  :FilterBasedQuery(config.sub("filter"), table),
  dimension_(table.dimension(config.str("dimension"))),
  term_(config.str("term")),
  limit_(config.num("limit")){
}

void SearchQuery::Accept(QueryVisitor& visitor) {
  visitor.Visit(this);
}

Query* QueryFactory::Create(const util::Config& config, db::Database& database) {
  std::string type = config.str("type");
  auto table = database.GetTable(config.str("table"));
  if (type == "aggregate") {
    return new AggregateQuery(config, *table);
  }
  if (type == "search") {
    return new SearchQuery(config, *table);
  }
  throw std::invalid_argument("Unsupported query type: " + type);
}

}}

