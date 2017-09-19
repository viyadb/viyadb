#include "query/runner.h"
#include "codegen/query/filter.h"
#include "codegen/query/query.h"

namespace viya {
namespace query {

namespace cg = viya::codegen;

void QueryRunner::Visit(AggregateQuery* query) {
  stats_.OnBegin("aggregate", query->table().name());

  auto query_fn = cg::QueryGenerator(database_.compiler(), *query).AggQueryFunction();

  cg::FilterArgsPacker args_packer;
  query->filter()->Accept(args_packer);

  stats_.OnCompile();

  query_fn(query->table(), output_, stats_, args_packer.args(), query->skip(), query->limit());
  stats_.OnEnd();
}

void QueryRunner::Visit(SearchQuery* query) {
  stats_.OnBegin("search", query->table().name());

  auto query_fn = cg::QueryGenerator(database_.compiler(), *query).SearchQueryFunction();

  cg::FilterArgsPacker args_packer;
  query->filter()->Accept(args_packer);

  stats_.OnCompile();

  query_fn(query->table(), output_, stats_, args_packer.args(), query->term(), query->limit());
  stats_.OnEnd();
}

}}
