#ifndef VIYA_CODEGEN_DB_QUERY_H_
#define VIYA_CODEGEN_DB_QUERY_H_

#include "query/query.h"
#include "query/runner.h"
#include "codegen/generator.h"

namespace viya {
namespace codegen {

namespace query = viya::query;

class ScanGenerator: public query::QueryVisitor {
  public:
    ScanGenerator(Code& code):code_(code) {}

    void Visit(query::AggregateQuery* query);
    void Visit(query::SearchQuery* query);

  private:
    void IterationStart(query::FilterBasedQuery* query);
    void IterationEnd();

    void UnpackArguments(query::FilterBasedQuery* query);

    void Scan(query::AggregateQuery* query);
    void Scan(query::SearchQuery* query);

    void SortResults(query::AggregateQuery* query);
    void SortResults(query::SearchQuery* query);

    void Materialize(query::AggregateQuery* query);
    void Materialize(query::SearchQuery* query);

  private:
    Code& code_;
};

class QueryGenerator: public FunctionGenerator {
  public:
    QueryGenerator(Compiler& compiler, query::Query& query)
      :FunctionGenerator(compiler),query_(query) {}

    QueryGenerator(const QueryGenerator& other) = delete;

    Code GenerateCode() const;
    query::AggQueryFn AggQueryFunction();
    query::SearchQueryFn SearchQueryFunction();

  private:
    query::Query& query_;
};

}}

#endif // VIYA_CODEGEN_DB_QUERY_H_
