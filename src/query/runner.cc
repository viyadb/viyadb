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

#include "db/table.h"
#include "query/runner.h"
#include "codegen/query/filter.h"
#include "codegen/query/agg_query.h"
#include "codegen/query/search_query.h"

namespace viya {
namespace query {

namespace cg = viya::codegen;

void QueryRunner::Visit(AggregateQuery* query) {
  stats_.OnBegin("aggregate", query->table().name());

  auto query_fn = cg::AggQueryGenerator(database_.compiler(), *query).Function();

  cg::FilterArgsPacker filter_args;
  query->filter()->Accept(filter_args);

  cg::FilterArgsPacker having_args;
  if (query->having() != nullptr) {
    query->having()->Accept(having_args);
  }

  stats_.OnCompile();

  query_fn(query->table(), output_, stats_, filter_args.args(), query->skip(), query->limit(),
           having_args.args());
  stats_.OnEnd();
}

void QueryRunner::Visit(SearchQuery* query) {
  stats_.OnBegin("search", query->table().name());

  auto query_fn = cg::SearchQueryGenerator(database_.compiler(), *query).Function();

  cg::FilterArgsPacker filter_args;
  query->filter()->Accept(filter_args);

  stats_.OnCompile();

  query_fn(query->table(), output_, stats_, filter_args.args(), query->term(), query->limit());
  stats_.OnEnd();
}

void QueryRunner::Visit(ShowTablesQuery* query) {
  RowOutput::Row tables;
  for (auto& ent : query->db().tables()) {
    tables.push_back(ent.first);
  }
  output_.Start();
  output_.SendAsCol(tables);
  output_.Flush();
}

}}
