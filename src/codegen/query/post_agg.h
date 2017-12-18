#ifndef VIYA_CODEGEN_DB_POST_AGG_H_
#define VIYA_CODEGEN_DB_POST_AGG_H_

#include "query/query.h"
#include "codegen/generator.h"

namespace viya {
namespace codegen {

namespace query = viya::query;

class PostAggVisitor: public query::QueryVisitor {
  public:
    PostAggVisitor(Code& code):code_(code) {}

    void Visit(query::AggregateQuery* query);
    void Visit(query::SearchQuery* query);

  private:
    void SortResults(query::AggregateQuery* query);
    void SortResults(query::SearchQuery* query);

  private:
    Code& code_;
};

class PostAggGenerator: public FunctionGenerator {
  public:
    PostAggGenerator(Compiler& compiler, query::TableQuery& query)
      :FunctionGenerator(compiler),query_(query) {}

    Code GenerateCode() const;

  private:
    query::TableQuery& query_;
};

}}

#endif // VIYA_CODEGEN_DB_POST_AGG_H_
