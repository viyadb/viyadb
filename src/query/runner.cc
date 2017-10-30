#include "db/table.h"
#include "query/runner.h"
#include "codegen/query/filter.h"
#include "codegen/query/query.h"

namespace viya {
namespace query {

namespace cg = viya::codegen;

void QueryRunner::Visit(AggregateQuery* query) {
  stats_.OnBegin("aggregate", query->table().name());

  auto query_fn = cg::QueryGenerator(database_.compiler(), *query).AggQueryFunction();

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

  auto query_fn = cg::QueryGenerator(database_.compiler(), *query).SearchQueryFunction();

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
