#ifndef VIYA_CODEGEN_DB_AGG_QUERY_H_
#define VIYA_CODEGEN_DB_AGG_QUERY_H_

#include "query/query.h"
#include "query/runner.h"
#include "codegen/generator.h"

namespace viya {
namespace codegen {

namespace query = viya::query;

class AggQueryGenerator: public FunctionGenerator {
  public:
    AggQueryGenerator(Compiler& compiler, query::AggregateQuery& query)
      :FunctionGenerator(compiler),query_(query) {}

    AggQueryGenerator(const AggQueryGenerator& other) = delete;

    Code GenerateCode() const;
    query::AggQueryFn Function();

  private:
    query::AggregateQuery& query_;
};

}}

#endif // VIYA_CODEGEN_DB_AGG_QUERY_H_
