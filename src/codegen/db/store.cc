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

#include "codegen/db/store.h"
#include "db/column.h"
#include "db/defs.h"
#include "db/table.h"

namespace viya {
namespace codegen {

namespace db = viya::db;

Code DimensionsStruct::GenerateCode() const {
  Code code;
  code.AddHeaders({"cstdio"});

  code << "struct " << struct_name_ << " {\n";

  // Field declarations:
  const db::Dimension *last_dim = nullptr;
  for (auto *dim : dimensions_) {
    code << " " << dim->num_type().cpp_type() << " _"
         << std::to_string(dim->index()) << ";\n";
    last_dim = dim;
  }

  // Equality operator:
  code << " bool operator==(const " << struct_name_ << " &other) const {\n";
  code << "  return \n";
  if (dimensions_.empty()) {
    code << "   true;\n";
  } else {
    for (auto *dim : dimensions_) {
      auto dim_idx = std::to_string(dim->index());
      code << "   _" << dim_idx << "==other._" << dim_idx
           << (dim == last_dim ? ";" : " &&") << "\n";
    }
  }
  code << " }\n";

#if ENABLE_PERSISTENCE
  // Save function:
  code << " void save(std::ostream& os) const {\n";
  for (auto *dim : dimensions_) {
    code << "  os.write(reinterpret_cast<const char*>(&_"
         << std::to_string(dim->index()) << "), "
         << std::to_string(dim->num_type().size()) << ");\n";
  }
  code << " }\n";

  // Load function:
  code << " void load(std::istream& is) {\n";
  for (auto *dim : dimensions_) {
    code << "  is.read(reinterpret_cast<char*>(&_"
         << std::to_string(dim->index()) << "), "
         << std::to_string(dim->num_type().size()) << ");\n";
  }
  code << " }\n";
#endif

  code << "};\n";

  // Hash generator functor:
  code << "struct " << struct_name_ << "Hasher {\n";
  code << " std::size_t operator()(const " << struct_name_ << "& k) const {\n";
  code << "  size_t h = 0L;\n";
  for (auto *dim : dimensions_) {
    code << "  h ^= ";
    if (dim->dim_type() == db::Dimension::DimType::NUMERIC &&
        static_cast<const db::NumDimension *>(dim)->fp()) {
      code << "std::hash<" << dim->num_type().cpp_type() << ">{} (k._"
           << std::to_string(dim->index()) << ")";
    } else {
      code << "k._" << std::to_string(dim->index());
    }
    code << " + 0x9e3779b9 + (h<<6) + (h>>2);\n";
  }
  code << "  return h;\n";
  code << " }\n};\n";

  return code;
}

Code MetricsStruct::GenerateCode() const {
  Code code;
  code.AddHeaders({"cstdint", "cstddef", "cfloat"});

  for (auto *metric : metrics_) {
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code.AddHeaders({"util/bitset.h"});
      code << "namespace util = viya::util;\n";
      break;
    }
  }

  code << "struct " << struct_name_ << " {\n";

  // Field declarations:
  bool has_count_metric = false;
  bool has_avg_metric = false;
  for (auto *metric : metrics_) {
    auto metric_idx = std::to_string(metric->index());
    auto &num_type = metric->num_type();
    code << " ";
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code << "util::Bitset<" << std::to_string(num_type.size()) << "> _"
           << metric_idx;
    } else {
      code << num_type.cpp_type() << " _" << metric_idx << "=";
      switch (metric->agg_type()) {
      case db::Metric::AggregationType::MAX:
        code << num_type.cpp_min_value();
        break;
      case db::Metric::AggregationType::MIN:
        code << num_type.cpp_max_value();
        break;
      default:
        code << "0";
        break;
      }
      if (metric->agg_type() == db::Metric::AggregationType::AVG) {
        has_avg_metric = true;
      } else if (metric->agg_type() == db::Metric::AggregationType::COUNT) {
        has_count_metric = true;
      }
    }
    code << ";\n";
  }
  if (has_avg_metric && !has_count_metric) {
    // Add special count metric for calculating averages:
    code << " uint64_t _count = 0;\n";
  }

  // Aggregate function:
  code << " void Update(const " << struct_name_ << " &metrics) {\n";
  for (auto *metric : metrics_) {
    auto metric_idx = std::to_string(metric->index());
    code << "  _" << metric_idx;
    switch (metric->agg_type()) {
    case db::Metric::AggregationType::SUM:
    case db::Metric::AggregationType::AVG:
    case db::Metric::AggregationType::COUNT:
      code << " += metrics._" << metric_idx;
      break;
    case db::Metric::AggregationType::MAX:
      code << " = std::max(_" << metric_idx << ", metrics._" << metric_idx
           << ")";
      break;
    case db::Metric::AggregationType::MIN:
      code << " = std::min(_" << metric_idx << ", metrics._" << metric_idx
           << ")";
      break;
    case db::Metric::AggregationType::BITSET:
      code << " |= metrics._" << metric_idx;
      break;
    default:
      throw std::runtime_error("Unsupported metric aggregation type!");
    }
    code << ";\n";
  }
  if (has_avg_metric && !has_count_metric) {
    code << "  _count += metrics._count;\n";
  }
  code << " }\n";

#if ENABLE_PERSISTENCE
  // Save function:
  code
      << " void save(std::ostream& os, char** buf, size_t& buf_size) const {\n";
  for (auto *metric : metrics_) {
    std::string field = "_" + std::to_string(metric->index());
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code << "  {\n";
      code << "   size_t s = " << field << ".size();\n";
      code << "   if (s > buf_size) { *buf = static_cast<char*>(realloc(*buf, "
              "s)); buf_size = s; }\n";
      code << "   " << field << ".write(*buf);\n";
      code << "   os.write(reinterpret_cast<const char*>(&s), "
              "sizeof(size_t));\n";
      code << "   os.write(*buf, s);\n";
      code << "  }\n";
    } else {
      code << "  os.write(reinterpret_cast<const char*>(&" << field << "), "
           << std::to_string(metric->num_type().size()) << ");\n";
    }
  }
  code << " }\n";

  // Load function:
  code << " void load(std::istream& is, char** buf, size_t& buf_size) {\n";
  for (auto *metric : metrics_) {
    std::string field = "_" + std::to_string(metric->index());
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code << "  {\n";
      code << "   size_t s;\n";
      code << "   is.read(reinterpret_cast<char*>(&s), sizeof(size_t));\n";
      code << "   if (s > buf_size) { *buf = static_cast<char*>(realloc(*buf, "
              "s)); buf_size = s; }\n";
      code << "   is.read(*buf, s);\n";
      code << "   " << field << ".read(*buf);\n";
      code << "  }\n";
    } else {
      code << "  is.read(reinterpret_cast<char*>(&" << field << "), "
           << std::to_string(metric->num_type().size()) << ");\n";
    }
  }
  code << " }\n";
#endif

  code << "};\n";

  return code;
}

Code SegmentStatsStruct::GenerateCode() const {
  Code code;
  code << "struct SegmentStats {\n";
  for (auto *dim : table_.dimensions()) {
    if (dim->dim_type() == db::Dimension::DimType::NUMERIC ||
        dim->dim_type() == db::Dimension::DimType::TIME) {
      auto dim_idx = std::to_string(dim->index());
      auto &num_type = dim->num_type();
      code << " " << num_type.cpp_type() << " dmax" << dim_idx << " = "
           << num_type.cpp_min_value() << ";\n";
      code << " " << num_type.cpp_type() << " dmin" << dim_idx << " = "
           << num_type.cpp_max_value() << ";\n";
    }
  }

  // Update function:
  code << " void Update(const Dimensions& dims) {\n";
  for (auto *dim : table_.dimensions()) {
    if (dim->dim_type() == db::Dimension::DimType::NUMERIC ||
        dim->dim_type() == db::Dimension::DimType::TIME) {
      auto dim_idx = std::to_string(dim->index());
      code << "  dmax" << dim_idx << " = std::max(dims._" << dim_idx << ", dmax"
           << dim_idx << ");\n";
      code << "  dmin" << dim_idx << " = std::min(dims._" << dim_idx << ", dmin"
           << dim_idx << ");\n";
    }
  }
  code << " }\n";
  code << "};\n";
  return code;
}

Code StoreDefs::GenerateCode() const {
  Code code;
  code.AddHeaders({"db/segment.h"});
  code << "namespace db = viya::db;\n";

  DimensionsStruct dims_struct(table_.dimensions(), "Dimensions");
  code << dims_struct.GenerateCode();

  MetricsStruct metrics_struct(table_.metrics(), "Metrics");
  code << metrics_struct.GenerateCode();

  SegmentStatsStruct stats_struct(table_);
  code << stats_struct.GenerateCode();

  auto size = std::to_string(table_.segment_size());
  code << "class Segment: public db::SegmentBase {\n";
  code << "public:\n";
  code << " Dimensions* d;\n";
  code << " Metrics* m;\n";
  code << " SegmentStats stats;\n";

  code << " Segment():SegmentBase(" << size << ") {\n";
  code << "  d = new Dimensions[" << size << "];\n";
  code << "  m = new Metrics[" << size << "];\n";
  code << " }\n";

  code << " ~Segment() {\n";
  code << "  delete[] d;\n";
  code << "  delete[] m;\n";
  code << " }\n";

  code << " void insert(Dimensions& dims, Metrics& metrics) {\n";
  code << "  lock_.lock();\n";
  code << "  d[size_] = dims;\n";
  code << "  m[size_] = metrics;\n";
  code << "  ++size_;\n";
  code << "  lock_.unlock();\n";
  code << " }\n";

#if ENABLE_PERSISTENCE
  code << " void save(std::ostream& os) {\n";
  code << "  char* buf = nullptr;\n";
  code << "  size_t buf_size = 0L;\n";
  code << "  for (size_t i = 0; i < size_; ++i) {\n";
  code << "   d[i].save(os);\n";
  code << "   m[i].save(os, &buf, buf_size);\n";
  code << "  }\n";
  code << "  if (buf != nullptr) { free(buf); }\n";
  code << " }\n";

  code << " void load(std::istream& is) {\n";
  code << "  char* buf = nullptr;\n";
  code << "  size_t buf_size = 0L;\n";
  code << "  for (size_t i = 0; i < size_; ++i) {\n";
  code << "   d[i].load(is);\n";
  code << "   m[i].load(is, &buf, buf_size);\n";
  code << "  }\n";
  code << "  if (buf != nullptr) { free(buf); }\n";
  code << " }\n";
#endif

  code << "};\n";
  return code;
}

Code CreateSegment::GenerateCode() const {
  Code code;
  StoreDefs store_defs(table_);
  code << store_defs.GenerateCode();
  code << "extern \"C\" db::SegmentBase* viya_segment_create() "
          "__attribute__((__visibility__(\"default\")));\n";
  code << "extern \"C\" db::SegmentBase* viya_segment_create() {\n";
  code << " return new Segment();\n";
  code << "}\n";
  return code;
}

db::CreateSegmentFn CreateSegment::Function() {
  return GenerateFunction<db::CreateSegmentFn>(
      std::string("viya_segment_create"));
}
} // namespace codegen
} // namespace viya
