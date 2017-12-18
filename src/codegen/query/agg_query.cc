#include "db/defs.h"
#include "codegen/db/store.h"
#include "codegen/query/scan.h"
#include "codegen/query/post_agg.h"
#include "codegen/query/agg_query.h"

namespace viya {
namespace codegen {

Code AggQueryGenerator::GenerateCode() const {
  Code code;
  code.AddHeaders({
    "unordered_map", "query/output.h", "query/stats.h", "db/table.h",
    "db/dictionary.h", "db/store.h", "util/format.h", "util/string.h"
  });

  code<<"namespace db = viya::db;\n";
  code<<"namespace query = viya::query;\n";
  code<<"namespace util = viya::util;\n";

  code<<"extern \"C\" void viya_query_agg(db::Table& table, query::RowOutput& output, query::QueryStats& stats,"
    <<"std::vector<db::AnyNum> fargs, size_t skip, size_t limit, std::vector<db::AnyNum> hargs) __attribute__((__visibility__(\"default\")));\n";

  code<<"extern \"C\" void viya_query_agg(db::Table& table, query::RowOutput& output, query::QueryStats& stats,"
    <<"std::vector<db::AnyNum> fargs, size_t skip, size_t limit, std::vector<db::AnyNum> hargs) {\n";

  std::vector<const db::Dimension*> dims;
  for (auto& dim_col : query_.dimension_cols()) {
    dims.push_back(dim_col.dim());
  }
  DimensionsStruct dims_struct(dims, "AggDimensions");
  code<<dims_struct.GenerateCode();

  std::vector<const db::Metric*> metrics;
  for (auto& metric_col : query_.metric_cols()) {
    metrics.push_back(metric_col.metric());
  }
  MetricsStruct metrics_struct(metrics, "AggMetrics");
  code<<metrics_struct.GenerateCode();

  StoreDefs store_defs(query_.table());
  code<<store_defs.GenerateCode();

  ScanVisitor scan_visitor(code);
  query_.Accept(scan_visitor);

  PostAggGenerator postagg_generator(compiler_, query_);
  code<<postagg_generator.GenerateCode();

  code<<"}\n";
  return code;
}

query::AggQueryFn AggQueryGenerator::Function() {
  return GenerateFunction<query::AggQueryFn>(std::string("viya_query_agg"));
}

}}

