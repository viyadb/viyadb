#include "db/column.h"
#include "codegen/query/filter.h"
#include "codegen/query/post_agg.h"

namespace viya {
namespace codegen {

void PostAggVisitor::SortResults(query::AggregateQuery* query) {
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

void PostAggVisitor::Visit(query::AggregateQuery* query) {
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

  // Apply HAVING filter:
  if (query->having() != nullptr) {
    FilterComparison comparison(query->having(), "harg");
    code_<<"  auto& tuple_dims = agg_it->first;\n";
    code_<<"  auto& tuple_metrics = agg_it->second;\n";
    code_<<"  auto r = "<<comparison.GenerateCode()<<";\n";
    code_<<"  if (!r) continue;\n";
  }

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

  // Detect count column to divide by for calculating averages:
  std::string count_field("count");
  for (auto& metric_col : query->metric_cols()) {
    if (metric_col.metric()->agg_type() == db::Metric::AggregationType::COUNT) {
      count_field = std::to_string(metric_col.metric()->index());
      break;
    }
  }

  // Output metrics:
  for (auto& metric_col : query->metric_cols()) {
    auto metric = metric_col.metric();
    auto metric_idx = std::to_string(metric->index());
    code_<<"  row["<<std::to_string(metric_col.index())<<"] = fmt.num(agg_it->second._"<<metric_idx;
    if (metric->agg_type() == db::Metric::AggregationType::AVG) {
      code_<<"/(double)agg_it->second._"<<count_field;
    } else if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
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
}

void PostAggVisitor::SortResults(query::SearchQuery* query __attribute__((unused))) {
}

void PostAggVisitor::Visit(query::SearchQuery* query __attribute__((unused))) {
  // Iterate on codes, and materialize output records:
  code_<<" output.SendAsCol(values);\n";
  code_<<" stats.output_recs = values.size();\n";
}

Code PostAggGenerator::GenerateCode() const {
  Code code;
  code<<"output.Start();\n";

  PostAggVisitor visitor(code);
  query_.Accept(visitor);

  code<<"output.Flush();\n";
  return code;
}

}}

