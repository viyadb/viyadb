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

#include "codegen/query/header.h"
#include "db/column.h"

namespace viya {
namespace codegen {

void HeaderGenerator::Visit(query::SelectQuery *query) {
  if (query->header()) {
    for (auto &dim_col : query->dimension_cols()) {
      code_ << "row[" << std::to_string(dim_col.index())
            << "] = table.dimension(" << std::to_string(dim_col.dim()->index())
            << ")->name();\n";
    }
    for (auto &metric_col : query->metric_cols()) {
      code_ << "row[" << std::to_string(metric_col.index())
            << "] = table.metric("
            << std::to_string(metric_col.metric()->index()) << ")->name();\n";
    }
    code_ << "output.Send(row);\n";
  }
}

void HeaderGenerator::Visit(query::AggregateQuery *query) {
  Visit(static_cast<query::SelectQuery *>(query));
}

void HeaderGenerator::Visit(query::SearchQuery *query) {
  if (query->header()) {
    code_ << "output.Send(std::vector<std::string> { table.dimension("
          << std::to_string(query->dimension()->index()) << ")->name() });\n";
  }
}

} // namespace codegen
} // namespace viya
