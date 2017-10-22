#ifndef VIYA_DB_RUNNER_H_
#define VIYA_DB_RUNNER_H_

#include "db/database.h"
#include "query/query.h"
#include "query/output.h"
#include "query/stats.h"

namespace viya {
namespace query {

namespace db = viya::db;

using AggQueryFn = void (*)(db::Table&, RowOutput&, QueryStats&, std::vector<db::AnyNum>, size_t, size_t, std::vector<db::AnyNum>);
using SearchQueryFn = void (*)(db::Table&, RowOutput&, QueryStats&, std::vector<db::AnyNum>, const std::string&, size_t);

class QueryRunner: public QueryVisitor {
  public:
    QueryRunner(db::Database& database, RowOutput& output)
      :database_(database),output_(output),stats_(database.statsd()) {}

    void Visit(AggregateQuery* query);
    void Visit(SearchQuery* query);

    const QueryStats& stats() const { return stats_; }

  private:
    void RunFilterBasedQuery(FilterBasedQuery* query);

  private:
    db::Database& database_;
    RowOutput& output_;
    QueryStats stats_;
};

}}

#endif // VIYA_DB_RUNNER_H_
