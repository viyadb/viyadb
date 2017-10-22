#include "db/column.h"
#include "db/table.h"
#include "db/defs.h"
#include "codegen/db/store.h"

namespace viya {
namespace codegen {

namespace db = viya::db;

Code DimensionsStruct::GenerateCode() const {
  Code code;
  code.AddHeaders({"cstdio"});

  code<<"struct "<<struct_name_<<" {\n";

  // Field declarations:
  const db::Dimension* last_dim = nullptr;
  for (auto* dim : dimensions_) {
    code<<" "<<dim->num_type().cpp_type()<<" _"<<std::to_string(dim->index())<<";\n";
    last_dim = dim;
  }

  // Equality operator:
  code<<" bool operator==(const "<<struct_name_<<" &other) const {\n";
  code<<"  return \n";
  if (dimensions_.empty()) {
    code<<"   true;\n";
  } else {
    for (auto* dim : dimensions_) {
      auto dim_idx = std::to_string(dim->index());
      code<<"   _"<<dim_idx<<"==other._"<<dim_idx<<(dim == last_dim ? ";" : " &&")<<"\n";
    }
  }
  code<<" }\n";

#if ENABLE_PERSISTENCE
  // Save function:
  code<<" size_t Save(FILE* fp) const {\n";
  code<<"  size_t bytes = 0;\n";
  for (auto* dim : dimensions_) {
    code<<"  bytes += fwrite(&_"<<std::to_string(dim->index())<<", "<<std::to_string(dim->num_type().size())<<", 1, fp);\n";
  }
  code<<"  return bytes;\n";
  code<<" }\n";

  // Load function:
  code<<" size_t Load(FILE* fp) {\n";
  code<<"  size_t bytes = 0;\n";
  for (auto* dim : dimensions_) {
    code<<"  bytes += fread(&_"<<std::to_string(dim->index())<<", "<<std::to_string(dim->num_type().size())<<", 1, fp);\n";
  }
  code<<"  return bytes;\n";
  code<<" }\n";
#endif

  code<<"};\n";

  // Hash generator functor:
  code<<"struct "<<struct_name_<<"Hasher {\n";
  code<<" std::size_t operator()(const "<<struct_name_<<"& k) const {\n";
  code<<"  size_t h = 0L;\n";
  for (auto* dim : dimensions_) {
    code<<"  h ^= ";
    if (dim->dim_type() == db::Dimension::DimType::NUMERIC && static_cast<const db::NumDimension*>(dim)->fp()) {
      code<<"std::hash<"<<dim->num_type().cpp_type()<<">{} (k._"<<std::to_string(dim->index())<<")";
    } else {
      code<<"k._"<<std::to_string(dim->index());
    }
    code<<" + 0x9e3779b9 + (h<<6) + (h>>2);\n";
  }
  code<<"  return h;\n";
  code<<" }\n};\n";

  return code;
}

Code MetricsStruct::GenerateCode() const {
  Code code;
  code.AddHeaders({"cstdint", "cstddef", "cfloat"});

  for (auto* metric : metrics_) {
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code.AddHeaders({"util/bitset.h"});
      break;
    }
  }

  code<<"struct "<<struct_name_<<" {\n";

  // Field declarations:
  bool has_count_metric = false;
  bool has_avg_metric = false;
  for (auto* metric : metrics_) {
    auto metric_idx = std::to_string(metric->index());
    auto& num_type = metric->num_type();
    code<<" ";
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code<<"Bitset<"<<std::to_string(num_type.size())<<"> _"<<metric_idx;
    } else {
      code<<num_type.cpp_type()<<" _"<<metric_idx<<"=";
      switch (metric->agg_type()) {
        case db::Metric::AggregationType::MAX:
          code<<num_type.cpp_min_value();
          break;
        case db::Metric::AggregationType::MIN:
          code<<num_type.cpp_max_value();
          break;
        default:
          code<<"0";
          break;
      }
      if (metric->agg_type() == db::Metric::AggregationType::AVG) {
        has_avg_metric = true;
      } else if (metric->agg_type() == db::Metric::AggregationType::COUNT) {
        has_count_metric = true;
      }
    }
    code<<";\n";
  }
  if (has_avg_metric && !has_count_metric) {
    // Add special count metric for calculating averages:
    code<<" uint64_t _count = 0;\n";
  }

  // Aggregate function:
  code<<" void Update(const "<<struct_name_<<" &metrics) {\n";
  for (auto* metric : metrics_) {
    auto metric_idx = std::to_string(metric->index());
    code<<"  _"<<metric_idx;
    switch (metric->agg_type()) {
      case db::Metric::AggregationType::SUM:
      case db::Metric::AggregationType::AVG:
      case db::Metric::AggregationType::COUNT:
        code<<" += metrics._"<<metric_idx;
        break;
      case db::Metric::AggregationType::MAX:
        code<<" = std::max(_"<<metric_idx<<", metrics._"<<metric_idx<<")";
        break;
      case db::Metric::AggregationType::MIN:
        code<<" = std::min(_"<<metric_idx<<", metrics._"<<metric_idx<<")";
        break;
      case db::Metric::AggregationType::BITSET:
        code<<" |= metrics._"<<metric_idx;
        break;
      default:
        throw std::runtime_error("Unsupported metric aggregation type!");
    }
    code<<";\n";
  }
  if (has_avg_metric && !has_count_metric) {
    code<<"  _count += metrics._count;\n";
  }
  code<<" }\n";

#if ENABLE_PERSISTENCE
  // Save function:
  code<<" size_t Save(FILE* fp) const {\n";
  code<<"  size_t bytes = 0;\n";
  for (auto* metric : metrics_) {
    if (metric->agg_type() != db::Metric::AggregationType::BITSET) {
      code<<"  bytes += fwrite(&_"<<std::to_string(metric->index())<<", "<<std::to_string(metric->num_type().size())<<", 1, fp);\n";
    }
  }
  code<<"  return bytes;\n";
  code<<" }\n";

  // Load function:
  code<<" size_t Load(FILE* fp) {\n";
  code<<"  size_t bytes = 0;\n";
  for (auto* metric : metrics_) {
    if (metric->agg_type() != db::Metric::AggregationType::BITSET) {
      code<<"  bytes += fread(&_"<<std::to_string(metric->index())<<", "<<std::to_string(metric->num_type().size())<<", 1, fp);\n";
    }
  }
  code<<"  return bytes;\n";
  code<<" }\n";
#endif

  code<<"};\n";

  return code;
}

Code SegmentStatsStruct::GenerateCode() const {
  Code code;
  code<<"struct SegmentStats {\n";
  for (auto* dim : table_.dimensions()) {
    if (dim->dim_type() == db::Dimension::DimType::NUMERIC
        || dim->dim_type() == db::Dimension::DimType::TIME) {
      auto dim_idx = std::to_string(dim->index());
      auto& num_type = dim->num_type();
      code<<" "<<num_type.cpp_type()<<" dmax"<<dim_idx<<" = "<<num_type.cpp_min_value()<<";\n";
      code<<" "<<num_type.cpp_type()<<" dmin"<<dim_idx<<" = "<<num_type.cpp_max_value()<<";\n";
    }
  }

  // Update function:
  code<<" void Update(const Dimensions& dims) {\n";
  for (auto* dim : table_.dimensions()) {
    if (dim->dim_type() == db::Dimension::DimType::NUMERIC
        || dim->dim_type() == db::Dimension::DimType::TIME) {
      auto dim_idx = std::to_string(dim->index());
      code<<"  dmax"<<dim_idx<<" = std::max(dims._"<<dim_idx<<", dmax"<<dim_idx<<");\n";
      code<<"  dmin"<<dim_idx<<" = std::min(dims._"<<dim_idx<<", dmin"<<dim_idx<<");\n";
    }
  }
  code<<" }\n";
  code<<"};\n";
  return code;
}

Code StoreDefs::GenerateCode() const {
  Code code;
  code.AddHeaders({"db/segment.h"});
  code<<"namespace db = viya::db;\n";

  DimensionsStruct dims_struct(table_.dimensions(), "Dimensions");
  code<<dims_struct.GenerateCode();

  MetricsStruct metrics_struct(table_.metrics(), "Metrics");
  code<<metrics_struct.GenerateCode();

  SegmentStatsStruct stats_struct(table_);
  code<<stats_struct.GenerateCode();

  auto size = std::to_string(table_.segment_size());
  code<<"class Segment: public db::SegmentBase {\n";
  code<<"public:\n";
  code<<" Dimensions* d;\n";
  code<<" Metrics* m;\n";
  code<<" SegmentStats stats;\n";

  code<<" Segment():SegmentBase("<<size<<") {\n";
  code<<"  d = new Dimensions["<<size<<"];\n";
  code<<"  m = new Metrics["<<size<<"];\n";
  code<<" }\n";

  code<<" ~Segment() {\n";
  code<<"  delete[] d;\n";
  code<<"  delete[] m;\n";
  code<<" }\n";

  code<<" void insert(Dimensions& dims, Metrics& metrics) {\n";
  code<<"  lock_.lock();\n";
  code<<"  d[size_] = dims;\n";
  code<<"  m[size_] = metrics;\n";
  code<<"  ++size_;\n";
  code<<"  lock_.unlock();\n";
  code<<" }\n";

#if ENABLE_PERSISTENCE
  code<<" void save(FILE* fp) {\n";
  code<<"  for (size_t i = 0; i < size_; ++i) {\n";
  code<<"   d[i].Save(fp);\n";
  code<<"   m[i].Save(fp);\n";
  code<<"  }\n";
  code<<" }\n";

  code<<" void load(FILE* fp) {\n";
  code<<"  for (size_t i = 0; i < size_; ++i) {\n";
  code<<"   d[i].Load(fp);\n";
  code<<"   m[i].Load(fp);\n";
  code<<"  }\n";
  code<<" }\n";
#endif

  code<<"};\n";
  return code;
}

Code CreateSegment::GenerateCode() const {
  Code code;
  StoreDefs store_defs(table_);
  code<<store_defs.GenerateCode();
  code<<"extern \"C\" db::SegmentBase* viya_segment_create() __attribute__((__visibility__(\"default\")));\n";
  code<<"extern \"C\" db::SegmentBase* viya_segment_create() {\n";
  code<<" return new Segment();\n";
  code<<"}\n";
  return code;
}

db::CreateSegmentFn CreateSegment::Function() {
  return GenerateFunction<db::CreateSegmentFn>(std::string("viya_segment_create"));
}

}}

