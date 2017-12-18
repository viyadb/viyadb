#include "db/defs.h"
#include "codegen/db/store.h"
#include "codegen/query/search_query.h"
#include "codegen/query/scan.h"
#include "codegen/query/post_agg.h"

namespace viya {
namespace codegen {

Code SearchQueryGenerator::GenerateCode() const {
  Code code;
  code.AddHeaders({
    "unordered_set", "query/output.h", "query/stats.h", "db/table.h",
    "db/dictionary.h", "db/store.h", "util/format.h"
  });

  code<<"namespace db = viya::db;\n";
  code<<"namespace query = viya::query;\n";
  code<<"namespace util = viya::util;\n";

  code<<"extern \"C\" void viya_query_search(db::Table& table, query::RowOutput& output, query::QueryStats& stats, "
    <<"std::vector<db::AnyNum> fargs, const std::string& term, size_t limit) __attribute__((__visibility__(\"default\")));\n";

  code<<"extern \"C\" void viya_query_search(db::Table& table, query::RowOutput& output, query::QueryStats& stats, "
    <<"std::vector<db::AnyNum> fargs, const std::string& term, size_t limit) {\n";

  StoreDefs store_defs(query_.table());
  code<<store_defs.GenerateCode();

  ScanVisitor scan_visitor(code);
  query_.Accept(scan_visitor);

  PostAggGenerator postagg_generator(compiler_, query_);
  code<<postagg_generator.GenerateCode();

  code<<"}\n";
  return code;
}

query::SearchQueryFn SearchQueryGenerator::Function() {
  return GenerateFunction<query::SearchQueryFn>(std::string("viya_query_search"));
}

}}

