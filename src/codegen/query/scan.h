#ifndef VIYA_CODEGEN_DB_SCAN_H_
#define VIYA_CODEGEN_DB_SCAN_H_

#include "query/query.h"

namespace viya {
namespace codegen {

namespace query = viya::query;

class ScanVisitor: public query::QueryVisitor {
  public:
    ScanVisitor(Code& code):code_(code) {}

    void Visit(query::AggregateQuery* query);
    void Visit(query::SearchQuery* query);

  private:
    void UnpackArguments(query::AggregateQuery* query);
    void UnpackArguments(query::SearchQuery* query);

    void IterationStart(query::FilterBasedQuery* query);
    void IterationEnd();

  private:
    Code& code_;
};

}}

#endif // VIYA_CODEGEN_DB_SCAN_H_
