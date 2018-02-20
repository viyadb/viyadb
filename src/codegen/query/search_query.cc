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

#include "codegen/query/search_query.h"
#include "codegen/db/store.h"
#include "codegen/query/post_agg.h"
#include "codegen/query/scan.h"
#include "db/defs.h"

namespace viya {
namespace codegen {

Code SearchQueryGenerator::GenerateCode() const {
  Code code;
  code.AddHeaders({"unordered_set", "query/output.h", "query/stats.h",
                   "db/table.h", "db/dictionary.h", "db/store.h",
                   "util/format.h"});

  code.AddNamespaces(
      {"db = viya::db", "query = viya::query", "util = viya::util"});

  code << "extern \"C\" void viya_query_search(db::Table& table, "
          "query::RowOutput& output, query::QueryStats& stats, "
       << "std::vector<db::AnyNum> fargs, const std::string& term, size_t "
          "limit) __attribute__((__visibility__(\"default\")));\n";

  code << "extern \"C\" void viya_query_search(db::Table& table, "
          "query::RowOutput& output, query::QueryStats& stats, "
       << "std::vector<db::AnyNum> fargs, const std::string& term, size_t "
          "limit) {\n";

#ifdef NDEBUG
  code << "// ========= definitions ==========\n";
#endif

  StoreDefs store_defs(query_.table());
  code << store_defs.GenerateCode();

  ScanVisitor scan_visitor(code);
  query_.Accept(scan_visitor);

  PostAggVisitor postagg_visitor(code);
  query_.Accept(postagg_visitor);

  code << "}\n";
  return code;
}

query::SearchQueryFn SearchQueryGenerator::Function() {
  return GenerateFunction<query::SearchQueryFn>(
      std::string("viya_query_search"));
}

} // namespace codegen
} // namespace viya
