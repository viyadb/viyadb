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
#include "codegen/db/rollup.h"
#include "codegen/db/store.h"
#include "db/column.h"
#include "db/defs.h"
#include "db/segment.h"
#include "db/table.h"
#include <algorithm>

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
      code.AddNamespaces({"util = viya::util"});
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
  code.AddNamespaces({"db = viya::db"});

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

bool UpsertContextDefs::AddOptimize() const {
  bool has_bitset_metric = std::any_of(
      table_.metrics().cbegin(), table_.metrics().cend(),
      [](const db::Metric *metric) {
        return metric->agg_type() == db::Metric::AggregationType::BITSET;
      });
  return has_bitset_metric || !table_.cardinality_guards().empty();
}

bool UpsertContextDefs::HasTimeDimension() const {
  return std::any_of(table_.dimensions().cbegin(), table_.dimensions().cend(),
                     [](const db::Dimension *dim) {
                       return dim->dim_type() == db::Dimension::DimType::TIME;
                     });
}

Code UpsertContextDefs::ResetFunctionCode() const {
  Code code;
  code << "void Reset() {\n";
  code << " stats = db::UpsertStats();\n";
  if (HasTimeDimension()) {
    RollupReset rollup_reset(table_.dimensions());
    code << rollup_reset.GenerateCode();
  }
  code << "}\n";
  return code;
}

Code UpsertContextDefs::OptimizeFunctionCode() const {
  Code code;
  code << "void Optimize() {\n";
  for (auto &guard : table_.cardinality_guards()) {
    auto dim_idx = std::to_string(guard.dim()->index());
    code << " for (auto it = card_stats" << dim_idx
         << ".begin(); it != card_stats" << dim_idx << ".end(); ++it) {\n";
    code << "  it->second.optimize();\n";
    code << " }\n";
  }
  code << " updates_before_optimize = 1000000L;\n";
  code << "}\n";
  return code;
}

Code UpsertContextDefs::UpsertContextStruct() const {
  Code code;
  code.AddNamespaces({"util = viya::util"});

  code << "struct UpsertContext {\n";
  code << " Dimensions upsert_dims;\n";
  code << " Metrics upsert_metrics;\n";
  code << " std::unordered_map<Dimensions,size_t,DimensionsHasher> "
          "tuple_offsets;\n";

  bool add_optimize = AddOptimize();
  if (add_optimize) {
    code << "uint32_t updates_before_optimize = 1000000L;\n";
  }

  if (HasTimeDimension()) {
    RollupDefs rollup_defs(table_.dimensions());
    code << rollup_defs.GenerateCode();
  }

  auto &cardinality_guards = table_.cardinality_guards();
  for (auto &guard : cardinality_guards) {
    auto dim_idx = std::to_string(guard.dim()->index());
    std::string struct_name = "CardDimKey" + dim_idx;
    DimensionsStruct card_key_struct(guard.dimensions(), struct_name);
    code << card_key_struct.GenerateCode();
    code << " " << struct_name << " card_dim_key" << dim_idx << ";\n";

    code << " std::unordered_map<" << struct_name << ","
         << "util::Bitset<" << std::to_string(guard.dim()->num_type().size())
         << ">"
         << "," << struct_name << "Hasher> card_stats" << dim_idx << ";\n";
  }

  for (auto *dimension : table_.dimensions()) {
    auto dim_idx = std::to_string(dimension->index());
    if (dimension->dim_type() == db::Dimension::DimType::STRING) {
      code << " db::DimensionDict* dict" << dim_idx << ";\n";
      code << " db::DictImpl<" << dimension->num_type().cpp_type() << ">* v2c"
           << dim_idx << ";\n";
    }
  }

  for (auto *metric : table_.metrics()) {
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code << "util::Bitset<" << std::to_string(metric->num_type().size())
           << "> empty_bitset" << std::to_string(metric->index()) << ";\n";
    }
  }

  code << ResetFunctionCode();

  if (add_optimize) {
    code << OptimizeFunctionCode();
  }

  code << " db::UpsertStats stats;\n";
  code << "};\n";
  return code;
}

Code UpsertContextDefs::GenerateCode() const {
  Code code;
  code.AddHeaders({"db/store.h", "db/table.h", "db/dictionary.h"});

  auto &cardinality_guards = table_.cardinality_guards();
  if (!cardinality_guards.empty()) {
    code.AddHeaders({"util/bitset.h"});
  }

  code << UpsertContextStruct();
  return code;
}

Code StoreFunctions::GenerateCode() const {
  Code code;

  StoreDefs store_defs(table_);
  code << store_defs.GenerateCode();

  UpsertContextDefs defs(table_);
  code << defs.GenerateCode();

  code << "extern \"C\" void* viya_upsert_context(const db::Table &table) "
          "__attribute__((__visibility__(\"default\")));\n";
  code << "extern \"C\" void* viya_upsert_context(const db::Table &table) "
          "{\n";
  code << " UpsertContext* ctx = new UpsertContext();\n";
  for (auto *dimension : table_.dimensions()) {
    if (dimension->dim_type() == db::Dimension::DimType::STRING) {
      auto dim_idx = std::to_string(dimension->index());
      code << " ctx->dict" << dim_idx
           << " = static_cast<const db::StrDimension*>(table.dimension("
           << dim_idx << "))->dict();\n";
      code << " ctx->v2c" << dim_idx << " = reinterpret_cast<db::DictImpl<"
           << dimension->num_type().cpp_type() << ">*>(ctx->dict" << dim_idx
           << "->v2c());\n";
    }
  }
  code << " return static_cast<void*>(ctx);\n";
  code << "}\n";

  code << "extern \"C\" db::SegmentBase* viya_segment_create() "
          "__attribute__((__visibility__(\"default\")));\n";
  code << "extern \"C\" db::SegmentBase* viya_segment_create() {\n";
  code << " return new Segment();\n";
  code << "}\n";

  return code;
}

CreateSegmentFn StoreFunctions::CreateSegmentFunction() {
  return GenerateFunction<db::CreateSegmentFn>(
      std::string("viya_segment_create"));
}

UpsertContextFn StoreFunctions::UpsertContextFunction() {
  return GenerateFunction<UpsertContextFn>(std::string("viya_upsert_context"));
}

} // namespace codegen
} // namespace viya
