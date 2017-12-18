#ifndef VIYA_CODEGEN_DB_SEARCH_QUERY_H_
#define VIYA_CODEGEN_DB_SEARCH_QUERY_H_

#include "query/query.h"
#include "query/runner.h"
#include "codegen/generator.h"

namespace viya {
namespace codegen {

namespace query = viya::query;

class SearchQueryGenerator: public FunctionGenerator {
  public:
    SearchQueryGenerator(Compiler& compiler, query::SearchQuery& query)
      :FunctionGenerator(compiler),query_(query) {}

    SearchQueryGenerator(const SearchQueryGenerator& other) = delete;

    Code GenerateCode() const;
    query::SearchQueryFn Function();

  private:
    query::SearchQuery& query_;
};

}}

#endif // VIYA_CODEGEN_DB_SEARCH_QUERY_H_
