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

#include "db/defs.h"
#include "db/column.h"
#include "db/table.h"
#include "codegen/db/metadata.h"
#include "codegen/db/store.h"

namespace viya {
namespace codegen {

namespace db = viya::db;
namespace util = viya::util;

Code TableMetadata::GenerateCode() const {
  Code code;
  code.AddHeaders({"db/table.h", "db/store.h", "db/dictionary.h", "json.hpp"});

  StoreDefs store_defs(table_);
  code<<store_defs.GenerateCode();

  code<<"using json = nlohmann::json;\n";

  code<<"extern \"C\" void viya_table_metadata(db::Table& table, std::string& output) __attribute__((__visibility__(\"default\")));\n";
  code<<"extern \"C\" void viya_table_metadata(db::Table& table, std::string& output) {\n";
  code<<" json meta;\n";

  // Segments metadata
  code<<" unsigned long records = 0L;\n";
  code<<" meta[\"segments\"] = json::array();\n";
  code<<" for (auto* s : table.store()->segments_copy()) {\n";
  code<<"  auto segment = static_cast<Segment*>(s);\n";
  code<<"  records += segment->size();\n";
  code<<"  json segment_meta;\n";
  for (auto* dim : table_.dimensions()) {
    if (dim->dim_type() == db::Dimension::DimType::NUMERIC
        || dim->dim_type() == db::Dimension::DimType::TIME) {
      auto dim_idx = std::to_string(dim->index());
      code<<"  segment_meta[\""<<dim->name()<<"\"][\"max\"] = segment->stats.dmax"<<dim_idx<<";\n";
      code<<"  segment_meta[\""<<dim->name()<<"\"][\"min\"] = segment->stats.dmin"<<dim_idx<<";\n";
    }
  }
  code<<"  meta[\"segments\"].push_back(segment_meta);\n"; 
  code<<" }\n";
  code<<" meta[\"records_num\"] = records;\n";

  // Dimensions metadata
  code<<" meta[\"dimensions\"] = json::array();\n";
  code<<" for (auto* dim : table.dimensions()) {\n";
  code<<"  json col_meta;\n";
  code<<"  col_meta[\"name\"] = dim->name();\n";  
  code<<"  if (dim->dim_type() == db::Dimension::DimType::STRING) {\n";
  code<<"   auto dict = static_cast<const db::StrDimension*>(dim)->dict();\n";
  code<<"   dict->lock().lock_shared();\n";
  code<<"   col_meta[\"cardinality\"] = dict->c2v().size();\n";
  code<<"   dict->lock().unlock_shared();\n";
  code<<"  }\n";
  code<<"  meta[\"dimensions\"].push_back(col_meta);\n"; 
  code<<" }\n";

  // Metrics metadata
  code<<" meta[\"metrics\"] = json::array();\n";
  for (auto* metric: table_.metrics()) {
    code<<" {\n";
    code<<"  json col_meta;\n";
    code<<"  col_meta[\"name\"] = \""<<metric->name()<<"\";\n";
#if META_BITSET
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code<<"  unsigned long metric_card = 0L;\n";
      code<<"  for (auto* s : table.store()->segments_copy()) {\n";
      code<<"   auto segment_size = s->size();\n";
      code<<"   auto segment = static_cast<Segment*>(s);\n";
      code<<"   for (size_t tuple_idx = 0; tuple_idx < segment_size; ++tuple_idx) {\n";
      code<<"    metric_card += segment->m[tuple_idx]._"<<std::to_string(metric->index())<<".cardinality();\n";
      code<<"   }\n";
      code<<"  }\n";
      code<<"  col_meta[\"cardinality\"] = metric_card;\n";
    }
#endif
    code<<"  meta[\"metrics\"].push_back(col_meta);\n"; 
    code<<" }\n";
  }

  code<<" output = meta.dump();\n";
  code<<"}\n";

  return code;
}

TableMetadataFn TableMetadata::Function() {
  return GenerateFunction<TableMetadataFn>(std::string("viya_table_metadata"));
}

}}

