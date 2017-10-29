#ifndef VIYA_QUERY_QUERY_H_
#define VIYA_QUERY_QUERY_H_

#include "db/rollup.h"
#include "query/filter.h"
#include "util/config.h"

namespace viya {
namespace db {

class Column;
class Database;
class Dimension;
class Metric;
class Table;

}}

namespace viya {
namespace query {

namespace db = viya::db;
namespace util = viya::util;

class Query {
  public:
    Query(db::Table& table):table_(table) {}
    virtual ~Query() {}
    virtual void Accept(class QueryVisitor& visitor) = 0;

    db::Table& table() { return table_; }

  private:
    db::Table& table_;
};

class FilterBasedQuery: public Query {
  public:
    FilterBasedQuery(const util::Config& config, db::Table& table);

    virtual ~FilterBasedQuery() {
      delete filter_;
    }

    const Filter* filter() const { return filter_; }

  private:
    Filter* filter_;
};

class SortColumn {
  public:
    SortColumn(const db::Column* col, size_t index, bool ascending)
      :col_(col),index_(index),ascending_(ascending) {}

    const db::Column* col() const { return col_; }
    size_t index() const { return index_; }
    bool ascending() const { return ascending_; }

  private:
    const db::Column* col_;
    size_t index_;
    bool ascending_;
};

class OutputColumn {
  public:
    OutputColumn(size_t index):index_(index) {}

    size_t index() const { return index_; };

  private:
    size_t index_;
};

class DimOutputColumn: public OutputColumn {
  public:
    DimOutputColumn(const db::Dimension* dim, size_t index)
      :OutputColumn(index),dim_(dim) {}

    DimOutputColumn(const util::Config& config, const db::Dimension* dim, size_t index);

    const db::Dimension* dim() const { return dim_; };
    const std::string& format() const { return format_; }
    const db::Granularity& granularity() const { return granularity_; }

  private:
    std::string format_;
    db::Granularity granularity_;
    const db::Dimension* dim_;
};

class MetricOutputColumn: public OutputColumn {
  public:
    MetricOutputColumn(const db::Metric* metric, size_t index)
      :OutputColumn(index),metric_(metric) {}

    MetricOutputColumn(const util::Config& config __attribute__((unused)), const db::Metric* metric, size_t index)
      :OutputColumn(index),metric_(metric) {}

    const db::Metric* metric() const { return metric_; };

  private:
    const db::Metric* metric_;
};

class AggregateQuery: public FilterBasedQuery {
  public:
    AggregateQuery(const util::Config& config, db::Table& table);

    virtual ~AggregateQuery() {
      delete having_;
    }

    const std::vector<DimOutputColumn>& dimension_cols() const { return dimension_cols_; }
    const std::vector<MetricOutputColumn>& metric_cols() const { return metric_cols_; }
    const std::vector<SortColumn>& sort_cols() const { return sort_cols_; }
    size_t skip() const { return skip_; }
    size_t limit() const { return limit_; }
    bool header() const { return header_; }
    const Filter* having() const { return having_; }

    void Accept(class QueryVisitor& visitor);
 
  private:
    std::vector<DimOutputColumn> dimension_cols_;
    std::vector<MetricOutputColumn> metric_cols_;
    std::vector<SortColumn> sort_cols_;
    size_t skip_;
    size_t limit_;
    bool header_;
    Filter* having_;
};

class SearchQuery: public FilterBasedQuery {
  public:
    SearchQuery(const util::Config& config, db::Table& table);

    void Accept(class QueryVisitor& visitor);

    const db::Dimension* dimension() const { return dimension_; }
    const std::string& term() const { return term_; }
    size_t limit() const { return limit_; }

  private:
    const db::Dimension* dimension_;
    std::string term_;
    size_t limit_;
};

class QueryVisitor {
  public:
    virtual void Visit(AggregateQuery* query) = 0;
    virtual void Visit(SearchQuery* query) = 0;
};

class QueryFactory {
  public:
    Query* Create(const util::Config& config, db::Database& database);
};

}}

#endif // VIYA_QUERY_QUERY_H_
