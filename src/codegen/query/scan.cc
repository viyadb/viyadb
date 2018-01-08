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

#include "codegen/db/rollup.h"
#include "codegen/query/scan.h"
#include "codegen/query/filter.h"

namespace viya {
namespace codegen {

void ScanVisitor::UnpackArguments(query::AggregateQuery* query) {
  FilterArgsUnpack filter_args(query->table(), query->filter(), "farg");
  code_<<filter_args.GenerateCode();

  if (query->having() != nullptr) {
    FilterArgsUnpack having_args(query->table(), query->having(), "harg");
    code_<<having_args.GenerateCode();
  }
}

void ScanVisitor::UnpackArguments(query::SearchQuery* query) {
  FilterArgsUnpack args_unpack(query->table(), query->filter(), "farg");
  code_<<args_unpack.GenerateCode();
}

void ScanVisitor::IterationStart(query::FilterBasedQuery* query) {
  // Iterate on segments:
  code_<<"for (auto* s : table.store()->segments_copy()) {\n";
  code_<<" auto segment_size = s->size();\n";
  code_<<" stats.scanned_recs += segment_size;\n";
  code_<<" auto segment = static_cast<Segment*>(s);\n";

  // Check whether to skip this segment:
  SegmentSkip segment_skip(query->table(), query->filter());
  code_<<" auto process_segment = "<<segment_skip.GenerateCode()<<";\n";
  code_<<" if (!process_segment) continue;\n";
  code_<<" stats.scanned_segments++;\n";

  // Iterate on tuples:
  code_<<" for (size_t tuple_idx = 0; tuple_idx < segment_size; ++tuple_idx) {\n";
  code_<<"  Dimensions& tuple_dims = segment->d[tuple_idx];\n";
  code_<<"  Metrics& tuple_metrics = segment->m[tuple_idx];\n";

  // Apply filter, and check it's return code:
  // TODO : is it possible to do it without IF branch?
  FilterComparison comparison(query->table(), query->filter(), "farg");
  code_<<"  auto r = "<<comparison.GenerateCode()<<";\n";
  code_<<"  if (r) {\n";
}

void ScanVisitor::IterationEnd() {
  // Close iteration loop:
  code_<<"  }\n";
  code_<<" }\n";
  code_<<"}\n";
}

void ScanVisitor::Visit(query::AggregateQuery* query) {
  // Structures for aggragation in memory:
#ifdef NDEBUG
  code_<<"// ========= scan ==========\n";
#endif
  code_<<"AggDimensions agg_dims;\n";
  code_<<"AggMetrics agg_metrics;\n";
  code_<<"std::unordered_map<AggDimensions,AggMetrics,AggDimensionsHasher> agg_map;\n";

  UnpackArguments(query);

  std::vector<const db::Dimension*> dims;
  for (auto& dim_col : query->dimension_cols()) {
    dims.push_back(dim_col.dim());
  }
  RollupDefs rollup_defs(dims);
  code_<<rollup_defs.GenerateCode();

  RollupReset rollup_reset(dims);
  code_<<rollup_reset.GenerateCode();

  IterationStart(query);

  for (auto& dim_col : query->dimension_cols()) {
    auto dimension = dim_col.dim();
    auto dim_idx = std::to_string(dimension->index());

    bool time_rollup = false;
    if (dimension->dim_type() == db::Dimension::DimType::TIME) {
      auto time_dim = static_cast<const db::TimeDimension*>(dimension);

      if (!time_dim->rollup_rules().empty() || !dim_col.granularity().empty()) {
        time_rollup = true;

        code_<<"time"<<dim_idx<<".set_ts(tuple_dims._"<<dim_idx<<");\n";

        TimestampRollup ts_rollup(time_dim, "tuple_dims._" + dim_idx);
        code_<<ts_rollup.GenerateCode();

        if (!dim_col.granularity().empty()) {
          code_<<"time"<<dim_idx<<".trunc<static_cast<util::TimeUnit>("
            <<static_cast<int>(dim_col.granularity().time_unit())<<")>();\n";
        }
        code_<<"agg_dims._"<<dim_idx<<" = time"<<dim_idx<<".get_ts();\n";
      }
    }
    if (!time_rollup) {
      code_<<"agg_dims._"<<dim_idx<<" = tuple_dims._"<<dim_idx<<";\n";
    }
  }

  bool has_count_metric = false;
  bool has_avg_metric = false;
  for (auto& metric_col : query->metric_cols()) {
    auto metric_idx = std::to_string(metric_col.metric()->index());
    code_<<"agg_metrics._"<<metric_idx<<" = tuple_metrics._"<<metric_idx<<";\n";
    if (metric_col.metric()->agg_type() == db::Metric::AggregationType::AVG) {
      has_avg_metric = true;
    } else if (metric_col.metric()->agg_type() == db::Metric::AggregationType::COUNT) {
      has_count_metric = true;
    }
  }
  if (has_avg_metric && !has_count_metric) {
    code_<<"agg_metrics._count = tuple_metrics._count;\n";
  }

  code_<<"agg_map[agg_dims].Update(agg_metrics);\n";

  IterationEnd();

  code_<<"stats.aggregated_recs = agg_map.size();\n";
}

void ScanVisitor::Visit(query::SearchQuery* query) {
#ifdef NDEBUG
  code_<<"// ========= scan ==========\n";
#endif
  auto dim = query->dimension();
  auto dim_idx = std::to_string(dim->index());

  code_<<"std::unordered_set<"<<dim->num_type().cpp_type()<<"> codes;\n";
  code_<<"std::vector<std::string> values;\n";
  code_<<"std::string check_value;\n";
  if (dim->dim_type() == db::Dimension::DimType::STRING) {
    code_<<"auto dict = static_cast<const db::StrDimension*>(table.dimension("<<dim_idx<<"))->dict();\n";
  }
  code_<<"util::Format fmt;\n";

  UnpackArguments(query);

  IterationStart(query);

  code_<<"if (codes.insert(tuple_dims._"<<dim_idx<<").second) {\n";
  if (dim->dim_type() == db::Dimension::DimType::STRING) {
    code_<<" dict->lock().lock_shared();\n";
    code_<<" check_value = dict->c2v()[tuple_dims._"<<dim_idx<<"];\n";
    code_<<" dict->lock().unlock_shared();\n";
  } else {
    code_<<" check_value = fmt.num(tuple_dims._"<<dim_idx<<");\n";
  }
  code_<<" if (check_value.find(term) != std::string::npos) {\n";
  code_<<"   values.push_back(check_value);\n";
  code_<<"   if (values.size() >= limit) break;\n";
  code_<<" }\n";
  code_<<"}\n";

  IterationEnd();

  code_<<"stats.aggregated_recs = codes.size();\n";
}

}}

