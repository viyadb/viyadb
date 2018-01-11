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

#ifndef VIYA_DB_RUNNER_H_
#define VIYA_DB_RUNNER_H_

#include "db/database.h"
#include "query/output.h"
#include "query/query.h"
#include "query/stats.h"

namespace viya {
namespace query {

namespace db = viya::db;

using AggQueryFn = void (*)(db::Table &, RowOutput &, QueryStats &,
                            std::vector<db::AnyNum>, size_t, size_t,
                            std::vector<db::AnyNum>);
using SearchQueryFn = void (*)(db::Table &, RowOutput &, QueryStats &,
                               std::vector<db::AnyNum>, const std::string &,
                               size_t);

class QueryRunner : public QueryVisitor {
public:
  QueryRunner(db::Database &database, RowOutput &output)
      : database_(database), output_(output), stats_(database.statsd()) {}

  void Visit(AggregateQuery *query);
  void Visit(SearchQuery *query);
  void Visit(ShowTablesQuery *query);

  const QueryStats &stats() const { return stats_; }

private:
  void RunFilterBasedQuery(FilterBasedQuery *query);

private:
  db::Database &database_;
  RowOutput &output_;
  QueryStats stats_;
};
}
}

#endif // VIYA_DB_RUNNER_H_
