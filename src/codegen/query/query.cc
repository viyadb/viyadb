#include "db/defs.h"
#include "codegen/db/store.h"
#include "codegen/db/rollup.h"
#include "codegen/query/query.h"
#include "codegen/query/filter.h"

namespace viya {
namespace codegen {

void ScanGenerator::IterationStart(query::FilterBasedQuery* query) {
  // Iterate on segments:
  code_<<" for (auto* s : table.store()->segments_copy()) {\n";
  code_<<"  auto segment_size = s->size();\n";
  code_<<"  stats.scanned_recs += segment_size;\n";
  code_<<"  auto segment = static_cast<Segment*>(s);\n";

  // Check whether to skip this segment:
  SegmentSkip segment_skip(query->filter());
  code_<<"  auto process_segment = "<<segment_skip.GenerateCode()<<";\n";
  code_<<"  if (!process_segment) continue;\n";
  code_<<"  stats.scanned_segments++;\n";

  // Iterate on tuples:
  code_<<"  for (size_t tuple_idx = 0; tuple_idx < segment_size; ++tuple_idx) {\n";
  code_<<"   Dimensions& tuple_dims = segment->d[tuple_idx];\n";
  code_<<"   Metrics& tuple_metrics = segment->m[tuple_idx];\n";

  // Apply filter, and check it's return code:
  // TODO : is it possible to do it without IF branch?
  FilterComparison comparison(query->filter());
  code_<<"   auto res = "<<comparison.GenerateCode()<<";\n";
  code_<<"   if (res) {\n";
}

void ScanGenerator::IterationEnd() {
  // Close iteration loop:
  code_<<"   }\n";
  code_<<"  }\n";
  code_<<" }\n";
}

void ScanGenerator::UnpackArguments(query::FilterBasedQuery* query) {
  FilterArgsUnpack args_unpack(query->filter());
  code_<<args_unpack.GenerateCode();
}

void ScanGenerator::Scan(query::AggregateQuery* query) {
  // Structures for aggragation in memory:
  code_<<" AggDimensions agg_dims;\n";
  code_<<" AggMetrics agg_metrics;\n";
  code_<<" std::unordered_map<AggDimensions,AggMetrics,AggDimensionsHasher> agg_map;\n";

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

  for (auto& metric_col : query->metric_cols()) {
    auto metric_idx = std::to_string(metric_col.metric()->index());
    code_<<"agg_metrics._"<<metric_idx<<" = tuple_metrics._"<<metric_idx<<";\n";
  }

  code_<<"agg_map[agg_dims].Update(agg_metrics);\n";

  IterationEnd();

  code_<<" stats.aggregated_recs = agg_map.size();\n";
}

void ScanGenerator::SortResults(query::AggregateQuery* query) {
  auto sort_columns = query->sort_cols();
  if (!sort_columns.empty()) {

    // Sort the materialized records:
    code_<<" std::sort(post_agg.begin(), post_agg.end(), [](const Row& a, const Row& b) {\n";

    size_t sc_size = sort_columns.size();
    for (size_t i = 0; i < sc_size; ++i) {

      auto& sort_column = sort_columns[i];
      auto col_idx = std::to_string(sort_column.index());
      auto col = sort_column.col();

      if (col->sort_type() == db::Column::SortType::STRING) {
        auto sign = sort_column.ascending() ? "<" : ">";
        code_<<"  if (a["<<col_idx<<"] "<<sign<<" b["<<col_idx<<"]) return true;\n";
        if (i < sc_size - 1) {
          code_<<"  if (b["<<col_idx<<"] "<<sign<<" a["<<col_idx<<"])";
        }
      } else {
        std::string cmp_func = col->sort_type() == db::Column::SortType::INTEGER ? 
          (sort_column.ascending() ? "SmallerInt" : "GreaterInt") :
          (sort_column.ascending() ? "SmallerFloat" : "GreaterFloat");
        code_<<"  if (util::StringNumCmp::"<<cmp_func<<"(a["<<col_idx<<"], b["<<col_idx<<"])) return true;\n";
        if (i < sc_size - 1) {
          code_<<"  if (util::StringNumCmp::"<<cmp_func<<"(b["<<col_idx<<"], a["<<col_idx<<"]))";
        }
      }
      code_<<"  return false;\n";
    }
    code_<<" });\n";

    // Apply skip & limit during output:
    code_<<" auto post_agg_end = limit > 0 ? post_agg.begin() + skip + limit : post_agg.end();\n";
    code_<<" for (auto it = post_agg.begin() + skip; it != post_agg_end; ++it) {\n";
    code_<<"  output.Send(*it);\n";
    code_<<"  ++stats.output_recs;\n";
    code_<<" }\n";
  }
}

void ScanGenerator::Materialize(query::AggregateQuery* query) {
  for (auto& dim_col : query->dimension_cols()) {
    auto dim = dim_col.dim();
    auto dim_idx = std::to_string(dim->index());
    if (dim->dim_type() == db::Dimension::DimType::STRING) {
      code_<<" auto dict"<<dim_idx
        <<" = static_cast<const db::StrDimension*>(table.dimension("<<dim_idx<<"))->dict();\n";
    }
  }

  auto sort_columns = query->sort_cols();
  code_<<" typedef std::vector<std::string> Row;\n";
  code_<<" Row row("<<std::to_string(query->dimension_cols().size() + query->metric_cols().size())<<");\n";
  code_<<" util::Format fmt;\n";
  
  // Calculate how much records we should skip / drop:
  code_<<" skip = std::min(agg_map.size(), skip);\n";
  code_<<" limit = std::min(limit, agg_map.size() - skip);\n";

  code_<<" auto agg_it = agg_map.begin();\n";
  code_<<" auto agg_end = agg_map.end();\n";
  if (sort_columns.empty()) {
    // Just skip & limit on the agg_map:
    code_<<" std::advance(agg_it, skip);\n";
    code_<<" if (limit > 0) {\n";
    code_<<"  agg_end = agg_it;\n";
    code_<<"  std::advance(agg_end, limit);\n";
    code_<<" }\n";
  } else {
    // Create additional structure to keep materialized records that will be sorted:
    code_<<" std::vector<Row> post_agg;\n";
  }

  code_<<" output.Start();\n";

  // Print header if requested:
  if (query->header()) {
    for (auto& dim_col : query->dimension_cols()) {
      code_<<" row["<<std::to_string(dim_col.index())
        <<"] = table.dimension("<<std::to_string(dim_col.dim()->index())<<")->name();\n";
    }
    for (auto& metric_col : query->metric_cols()) {
      code_<<" row["<<std::to_string(metric_col.index())
        <<"] = table.metric("<<std::to_string(metric_col.metric()->index())<<")->name();\n";
    }
    code_<<" output.Send(row);\n";
  }

  // Iterate on agg_map, and materialize output records:
  code_<<" for (; agg_it != agg_end; ++agg_it) {\n";

  for (auto& dim_col : query->dimension_cols()) {
    auto dimension = dim_col.dim();
    auto dim_idx = std::to_string(dimension->index());
    auto col_idx = std::to_string(dim_col.index());

    if (dimension->dim_type() == db::Dimension::DimType::STRING) {
      code_<<"  dict"<<dim_idx<<"->lock().lock_shared();\n";
      code_<<"  row["<<col_idx<<"] = dict"<<dim_idx<<"->c2v()[agg_it->first._"<<dim_idx<<"];\n";
      code_<<"  dict"<<dim_idx<<"->lock().unlock_shared();\n";
    }
    else if (dimension->dim_type() == db::Dimension::DimType::TIME && !dim_col.format().empty()) {
      code_<<"  row["<<col_idx<<"] = fmt.date(\""<<dim_col.format()<<"\", agg_it->first._"<<dim_idx<<");\n";
    }
    else if (dimension->dim_type() == db::Dimension::DimType::BOOLEAN) {
      code_<<"  row["<<col_idx<<"] = agg_it->first._"<<dim_idx<<"? \"true\" : \"false\";\n";
    } else {
      code_<<"  row["<<col_idx<<"] = fmt.num(agg_it->first._"<<dim_idx<<");\n";
    }
  }

  for (auto& metric_col : query->metric_cols()) {
    auto metric = metric_col.metric();
    auto metric_idx = std::to_string(metric->index());
    code_<<"  row["<<std::to_string(metric_col.index())<<"] = fmt.num(agg_it->second._"<<metric_idx;
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code_<<".cardinality()";
    }
    code_<<");\n";
  }
  if (sort_columns.empty()) {
    code_<<"  output.Send(row);\n";
    code_<<"  ++stats.output_recs;\n";
  } else {
    code_<<"  post_agg.push_back(row);\n";
  }
  code_<<" }\n";

  SortResults(query);
  code_<<" output.Flush();\n";
}

void ScanGenerator::Visit(query::AggregateQuery* query) {
  code_.AddHeaders({
    "unordered_map", "query/output.h", "query/stats.h", "db/table.h",
      "db/dictionary.h", "db/store.h", "util/format.h", "util/string.h"
  });

  code_<<"namespace query = viya::query;\n";
  code_<<"namespace util = viya::util;\n";

  std::vector<const db::Dimension*> dims;
  for (auto& dim_col : query->dimension_cols()) {
    dims.push_back(dim_col.dim());
  }
  DimensionsStruct dims_struct(dims, "AggDimensions");
  code_<<dims_struct.GenerateCode();

  std::vector<const db::Metric*> metrics;
  for (auto& metric_col : query->metric_cols()) {
    metrics.push_back(metric_col.metric());
  }
  MetricsStruct metrics_struct(metrics, "AggMetrics");
  code_<<metrics_struct.GenerateCode();

  code_<<"extern \"C\" void viya_query_agg(db::Table& table, query::RowOutput& output, "
    <<"query::QueryStats& stats, std::vector<db::AnyNum> fargs, size_t skip, size_t limit) __attribute__((__visibility__(\"default\")));\n";

  code_<<"extern \"C\" void viya_query_agg(db::Table& table, query::RowOutput& output, "
    <<"query::QueryStats& stats, std::vector<db::AnyNum> fargs, size_t skip, size_t limit) {\n";

  UnpackArguments(query);
  Scan(query);
  Materialize(query);

  code_<<"}\n";
}

void ScanGenerator::Scan(query::SearchQuery* query) {
  auto dim = query->dimension();
  auto dim_idx = std::to_string(dim->index());

  code_<<" std::unordered_set<"<<dim->num_type().cpp_type()<<"> codes;\n";
  code_<<" std::vector<std::string> values;\n";
  code_<<" std::string check_value;\n";
  if (dim->dim_type() == db::Dimension::DimType::STRING) {
    code_<<" auto dict = static_cast<const db::StrDimension*>(table.dimension("<<dim_idx<<"))->dict();\n";
  }
  code_<<" util::Format fmt;\n";

  IterationStart(query);

  code_<<"if (codes.insert(tuple_dims._"<<dim_idx<<").second) {\n";
  if (dim->dim_type() == db::Dimension::DimType::STRING) {
    code_<<"  dict->lock().lock_shared();\n";
    code_<<"  check_value = dict->c2v()[tuple_dims._"<<dim_idx<<"];\n";
    code_<<"  dict->lock().unlock_shared();\n";
  } else {
    code_<<"  check_value = fmt.num(tuple_dims._"<<dim_idx<<");\n";
  }
  code_<<"  if (check_value.find(term) != std::string::npos) {\n";
  code_<<"    values.push_back(check_value);\n";
  code_<<"    if (values.size() >= limit) break;\n";
  code_<<"  }\n";
  code_<<"}\n";

  IterationEnd();

  code_<<" stats.aggregated_recs = codes.size();\n";
}

void ScanGenerator::SortResults(query::SearchQuery* query __attribute__((unused))) {
}

void ScanGenerator::Materialize(query::SearchQuery* query __attribute__((unused))) {
  // Iterate on codes, and materialize output records:
  code_<<" output.Start();\n";
  code_<<" output.SendAsCol(values);\n";
  code_<<" stats.output_recs = values.size();\n";
  code_<<" output.Flush();\n";
}

void ScanGenerator::Visit(query::SearchQuery* query) {
  code_.AddHeaders({
    "unordered_set", "query/output.h", "query/stats.h", "db/table.h",
    "db/dictionary.h", "db/store.h", "util/format.h"});

  code_<<"namespace query = viya::query;\n";
  code_<<"namespace util = viya::util;\n";

  code_<<"extern \"C\" void viya_query_search(db::Table& table, query::RowOutput& output, query::QueryStats& stats, "
    <<"std::vector<db::AnyNum> fargs, const std::string& term, size_t limit) __attribute__((__visibility__(\"default\")));\n";

  code_<<"extern \"C\" void viya_query_search(db::Table& table, query::RowOutput& output, query::QueryStats& stats, "
    <<"std::vector<db::AnyNum> fargs, const std::string& term, size_t limit) {\n";

  UnpackArguments(query);
  Scan(query);
  Materialize(query);

  code_<<"}\n";
}

Code QueryGenerator::GenerateCode() const {
  Code code;

  StoreDefs store_defs(query_.table());
  code<<store_defs.GenerateCode();

  ScanGenerator scan_generator(code);
  query_.Accept(scan_generator);

  return code;
}

query::AggQueryFn QueryGenerator::AggQueryFunction() {
  return GenerateFunction<query::AggQueryFn>(std::string("viya_query_agg"));
}

query::SearchQueryFn QueryGenerator::SearchQueryFunction() {
  return GenerateFunction<query::SearchQueryFn>(std::string("viya_query_search"));
}

}}

