/*
 * Copyright (c) 2017 ViyaDB Group
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

#include "db/column.h"
#include "codegen/query/filter.h"
#include "codegen/query/sort.h"

namespace viya {
namespace codegen {

void SortVisitor::Visit(query::AggregateQuery* query) {
  auto sort_columns = query->sort_cols();
  if (!sort_columns.empty()) {
#ifdef NDEBUG
    code_<<"// ========= sort ==========\n";
#endif

    // Sort the materialized records:
    code_<<"std::sort(post_agg.begin(), post_agg.end(), [](const Row& a, const Row& b) {\n";

    size_t sc_size = sort_columns.size();
    for (size_t i = 0; i < sc_size; ++i) {

      auto& sort_column = sort_columns[i];
      auto col_idx = std::to_string(sort_column.index());
      auto col = sort_column.col();

      if (col->sort_type() == db::Column::SortType::STRING) {
        auto sign = sort_column.ascending() ? "<" : ">";
        code_<<" if (a["<<col_idx<<"] "<<sign<<" b["<<col_idx<<"]) return true;\n";
        if (i < sc_size - 1) {
          code_<<" if (b["<<col_idx<<"] "<<sign<<" a["<<col_idx<<"])";
        }
      } else {
        std::string cmp_func = col->sort_type() == db::Column::SortType::INTEGER ? 
          (sort_column.ascending() ? "SmallerInt" : "GreaterInt") :
          (sort_column.ascending() ? "SmallerFloat" : "GreaterFloat");
        code_<<" if (util::StringNumCmp::"<<cmp_func<<"(a["<<col_idx<<"], b["<<col_idx<<"])) return true;\n";
        if (i < sc_size - 1) {
          code_<<" if (util::StringNumCmp::"<<cmp_func<<"(b["<<col_idx<<"], a["<<col_idx<<"]))";
        }
      }
      code_<<" return false;\n";
    }
    code_<<"});\n";

    // Apply skip & limit during output:
    code_<<"auto post_agg_end = limit > 0 ? post_agg.begin() + skip + limit : post_agg.end();\n";
    code_<<"for (auto it = post_agg.begin() + skip; it != post_agg_end; ++it) {\n";
    code_<<" output.Send(*it);\n";
    code_<<" ++stats.output_recs;\n";
    code_<<"}\n";
  }
}

void SortVisitor::Visit(query::SearchQuery* query __attribute__((unused))) {
}

}}

