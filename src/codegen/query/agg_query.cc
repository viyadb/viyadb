/*
 * Copyright (c) 2017-present ViyaDB Group
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

#include "codegen/query/agg_query.h"
#include "codegen/db/store.h"
#include "codegen/query/post_agg.h"
#include "codegen/query/scan.h"
#include "db/defs.h"

namespace viya {
namespace codegen {

Code AggQueryGenerator::GenerateCode() const {
  Code code;
  code.AddHeaders({"unordered_map", "query/output.h", "query/stats.h",
                   "db/table.h", "db/dictionary.h", "db/store.h",
                   "util/format.h", "util/string.h"});

  code.AddNamespaces(
      {"db = viya::db", "query = viya::query", "util = viya::util"});

  code << "extern \"C\" void viya_query_agg(db::Table& table, "
          "query::RowOutput& output, query::QueryStats& stats,"
          "std::vector<db::AnyNum> fargs, size_t skip, size_t limit, "
          "std::vector<db::AnyNum> hargs) "
          "__attribute__((__visibility__(\"default\")));\n";

  code << "extern \"C\" void viya_query_agg(db::Table& table, "
          "query::RowOutput& output, query::QueryStats& stats,"
          "std::vector<db::AnyNum> fargs, size_t skip, size_t limit, "
          "std::vector<db::AnyNum> hargs) {\n";

#ifndef NDEBUG
  code << "\n// ========= definitions ==========\n";
#endif
  std::vector<const db::Dimension *> dims;
  for (auto &dim_col : query_.dimension_cols()) {
    dims.push_back(dim_col.dim());
  }
  std::vector<const db::Metric *> metrics;
  for (auto &metric_col : query_.metric_cols()) {
    metrics.push_back(metric_col.metric());
  }
  TupleStruct tuple_struct(dims, metrics, "AggTuple");
  code << tuple_struct.GenerateCode();

  StoreDefs store_defs(query_.table());
  code << store_defs.GenerateCode();

  ScanVisitor scan_visitor(code);
  query_.Accept(scan_visitor);

  PostAggVisitor postagg_visitor(code);
  query_.Accept(postagg_visitor);

  code << "}\n";
  return code;
}

query::AggQueryFn AggQueryGenerator::Function() {
  return GenerateFunction<query::AggQueryFn>(std::string("viya_query_agg"));
}

} // namespace codegen
} // namespace viya
