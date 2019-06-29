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

Code TupleStruct::GenerateCode() const {
  Code code;
  code.AddHeaders({"cstdio", "cstdint", "cstddef", "cfloat"});

  code << "struct " << struct_name_ << " {\n";

  // ========================
  // Dimensions substructure
  // ========================
  code << " struct Dimensions {\n";

  // Dimension fields
  const db::Dimension *last_dim = nullptr;
  for (auto *dim : dimensions_) {
    code << "  " << dim->num_type().cpp_type() << " _"
         << std::to_string(dim->index()) << ";\n";
    last_dim = dim;
  }

  // Equality predicate functor:
  code << "  struct KeyEqual {\n"
          "   bool operator()("
          "const Dimensions &d1,const Dimensions &d2) const {\n"
          "    return \n";
  if (dimensions_.empty()) {
    code << "   true;\n";
  } else {
    for (auto *dim : dimensions_) {
      auto dim_idx = std::to_string(dim->index());
      code << "     d1._" << dim_idx << "==d2._" << dim_idx
           << (dim == last_dim ? ";" : " &&") << "\n";
    }
  }
  code << "   }\n";
  code << "  };\n";

  // Hash generator functor:
  code << "  struct Hash {\n"
          "   std::size_t operator()(const Dimensions &k) const {\n"
          "    size_t h = 0L;\n";
  for (auto *dim : dimensions_) {
    code << "    h ^= ";
    if (dim->dim_type() == db::Dimension::DimType::NUMERIC &&
        static_cast<const db::NumDimension *>(dim)->fp()) {
      code << "std::hash<" << dim->num_type().cpp_type() << ">{} (k._"
           << std::to_string(dim->index()) << ")";
    } else {
      code << "k._" << std::to_string(dim->index());
    }
    code << " + 0x9e3779b9 + (h<<6) + (h>>2);\n";
  }
  code << "    return h;\n"
          "   }\n"
          "  };\n"
          " };\n";

  // ======================
  // Metrics substructure
  // ======================
  code << " struct Metrics {\n";

  // Metric fields
  bool has_count_metric = false;
  bool has_avg_metric = false;

  for (auto *metric : metrics_) {
    auto metric_idx = std::to_string(metric->index());
    auto &num_type = metric->num_type();
    code << "  ";
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code.AddHeaders({"util/bitset.h"});
      code.AddNamespaces({"util = viya::util"});
      code << "util::Bitset<" << std::to_string(num_type.size()) << "> _"
           << metric_idx;
    } else {
      code << num_type.cpp_type() << " _" << metric_idx << " = ";
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
    code << "  uint64_t _count=0;\n";
  }

  // Tuple aggregate function:
  code << "  void Update(const Metrics &metrics) {\n";
  for (auto *metric : metrics_) {
    auto metric_idx = std::to_string(metric->index());
    code << "   _" << metric_idx;
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
    code << "   _count += metrics._count;\n";
  }
  code << "  }\n";
  code << " };\n";

  code << " Dimensions d;\n"
       << " Metrics m;\n"
       << "};\n";

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
           << num_type.cpp_min_value() << ";\n"
           << " " << num_type.cpp_type() << " dmin" << dim_idx << " = "
           << num_type.cpp_max_value() << ";\n";
    }
  }

  // Update function:
  code << " void Update(const Tuple& tuple) {\n";
  for (auto *dim : table_.dimensions()) {
    if (dim->dim_type() == db::Dimension::DimType::NUMERIC ||
        dim->dim_type() == db::Dimension::DimType::TIME) {
      auto dim_idx = std::to_string(dim->index());
      code << "  dmax" << dim_idx << " = std::max(tuple.d._" << dim_idx
           << ", dmax" << dim_idx << ");\n"
           << "  dmin" << dim_idx << " = std::min(tuple.d._" << dim_idx
           << ", dmin" << dim_idx << ");\n";
    }
  }
  code << " }\n";
  code << "};\n";
  return code;
}

Code StoreDefs::GenerateCode() const {
  Code code;
  code.AddHeaders({"db/segment.h", "cstdio", "cstdint", "cstddef", "cfloat"});
  code.AddNamespaces({"db = viya::db"});

  TupleStruct tuple_struct(table_.dimensions(), table_.metrics(), "Tuple");
  code << tuple_struct.GenerateCode();

  SegmentStatsStruct stats_struct(table_);
  code << stats_struct.GenerateCode();

  code << "class Segment: public db::SegmentBase {\n";

  std::string size = std::to_string(table_.segment_size());

  // ========================
  // Dimensions substructure
  // ========================
  code << " struct Dimensions {\n";

  // Dimension fields
  for (auto *dim : table_.dimensions()) {
    code << "  " << dim->num_type().cpp_type() << " _"
         << std::to_string(dim->index()) << "[" << size << "];\n";
  }

  // Tuple insert function:
  code << "  void Insert(const Tuple::Dimensions &dims, size_t tuple_idx) {\n";
  for (auto *dim : table_.dimensions()) {
    auto dim_idx = std::to_string(dim->index());
    code << "  _" << dim_idx << "[tuple_idx] = dims._" << dim_idx << ";\n";
  }
  code << "  }\n";
  code << " };\n";

  // ========================
  // Metrics substructure
  // ========================
  code << " struct Metrics {\n";

  // Metric fields
  bool has_count_metric = false;
  bool has_avg_metric = false;

  Code constructor_code;
  constructor_code << "  Metrics() {\n";

  for (auto *metric : table_.metrics()) {
    auto metric_idx = std::to_string(metric->index());
    auto &num_type = metric->num_type();

    code << "  ";
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code.AddHeaders({"util/bitset.h"});
      code.AddNamespaces({"util = viya::util"});
      code << "util::Bitset<" << std::to_string(num_type.size()) << "> _"
           << metric_idx << "[" << size << "]";
    } else {
      code << num_type.cpp_type() << " _" << metric_idx << "[" << size << "]";
      switch (metric->agg_type()) {
      case db::Metric::AggregationType::MAX:
        constructor_code << "  std::fill_n("
                         << "_" << metric_idx << "," << size << ","
                         << num_type.cpp_min_value() << ");\n";
        break;
      case db::Metric::AggregationType::MIN:
        constructor_code << "   std::fill_n("
                         << "_" << metric_idx << "," << size << ","
                         << num_type.cpp_max_value() << ");\n";
        break;
      default:
        code << " = {0}";
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
    code << "  uint64_t _count[" << size << "] = {0};\n";
  }

  constructor_code << "  }\n";
  code << constructor_code;

  // Tuple insert function:
  code << "  void Insert(const Tuple::Metrics &metrics, size_t tuple_idx) {\n";
  for (auto *metric : table_.metrics()) {
    auto metric_idx = std::to_string(metric->index());
    code << "   _" << metric_idx << "[tuple_idx] = metrics._" << metric_idx
         << ";\n";
  }
  if (has_avg_metric && !has_count_metric) {
    code << "   _count[tuple_idx] = metrics._count;\n";
  }
  code << "  }\n";

  // Tuple aggreagate function:
  code << "  void Update(const Tuple::Metrics &metrics, size_t tuple_idx) {\n";
  for (auto *metric : table_.metrics()) {
    auto metric_idx = std::to_string(metric->index());
    code << "   _" << metric_idx << "[tuple_idx]";
    switch (metric->agg_type()) {
    case db::Metric::AggregationType::SUM:
    case db::Metric::AggregationType::AVG:
    case db::Metric::AggregationType::COUNT:
      code << " += metrics._" << metric_idx;
      break;
    case db::Metric::AggregationType::MAX:
      code << " = std::max(_" << metric_idx << "[tuple_idx], metrics._"
           << metric_idx << ")";
      break;
    case db::Metric::AggregationType::MIN:
      code << " = std::min(_" << metric_idx << "[tuple_idx], metrics._"
           << metric_idx << ")";
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
    code << "   _count[tuple_idx] += metrics._count;\n";
  }
  code << "  }\n"
       << " };\n";

  code << "public:\n"
          " Dimensions d;\n"
          " Metrics m;\n"
          " SegmentStats stats;\n"
          " Segment():SegmentBase("
       << size
       << ") {}\n"
          " void Insert(Tuple& tuple) {\n"
          "  lock_.lock();\n"
          "  d.Insert(tuple.d, size_);\n"
          "  m.Insert(tuple.m, size_);\n"
          "  ++size_;\n"
          "  lock_.unlock();\n"
          " }\n";

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
  code << " void Reset() {\n"
          "  stats = db::UpsertStats();\n";
  if (HasTimeDimension()) {
    RollupReset rollup_reset(table_.dimensions());
    code << rollup_reset.GenerateCode();
  }
  code << " }\n";
  return code;
}

Code UpsertContextDefs::OptimizeFunctionCode() const {
  Code code;
  code << "void Optimize() {\n";
  for (auto &guard : table_.cardinality_guards()) {
    auto dim_idx = std::to_string(guard.dim()->index());
    code << " for (auto it = card_stats" << dim_idx
         << ".begin(); it != card_stats" << dim_idx
         << ".end(); ++it) {\n"
            "  it->second.optimize();\n"
            " }\n";
  }
  code << " updates_before_optimize = 1000000L;\n"
          "}\n";
  return code;
}

Code UpsertContextDefs::UpsertContextStruct() const {
  Code code;
  code.AddNamespaces({"util = viya::util"});

  code << "struct UpsertContext {\n"
          " Tuple upsert_tuple;\n"
          " std::unordered_map<Tuple::Dimensions,size_t,Tuple::Dimensions::"
          "Hash, Tuple::Dimensions::KeyEqual> tuple_offsets;\n";

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
    code << "struct " << struct_name << " {\n";

    // Dimension fields
    const db::Dimension *last_dim = nullptr;
    for (auto *dim : guard.dimensions()) {
      code << " " << dim->num_type().cpp_type() << " _"
           << std::to_string(dim->index()) << ";\n";
      last_dim = dim;
    }

    // Equality predicate functor:
    code << " struct KeyEqual {\n";
    code << "  bool operator()(const " << struct_name << " &d1, const "
         << struct_name << " &d2) const {\n";
    code << "   return \n";
    if (last_dim == nullptr) {
      code << "   true;\n";
    } else {
      for (auto *dim : guard.dimensions()) {
        auto dim_idx = std::to_string(dim->index());
        code << "   d1. _" << dim_idx << "==d2._" << dim_idx
             << (dim == last_dim ? ";" : " &&") << "\n";
      }
    }
    code << "  }\n";
    code << " };\n";

    // Hash generator functor:
    code << " struct Hash {\n";
    code << "  std::size_t operator()(const " << struct_name
         << "& k) const {\n";
    code << "   size_t h = 0L;\n";
    for (auto *dim : guard.dimensions()) {
      code << "   h ^= ";
      if (dim->dim_type() == db::Dimension::DimType::NUMERIC &&
          static_cast<const db::NumDimension *>(dim)->fp()) {
        code << "std::hash<" << dim->num_type().cpp_type() << ">{} (k._"
             << std::to_string(dim->index()) << ")";
      } else {
        code << "k._" << std::to_string(dim->index());
      }
      code << " + 0x9e3779b9 + (h<<6) + (h>>2);\n";
    }
    code << "   return h;\n"
            "  }\n"
            " };\n"
            "};\n";

    code << " " << struct_name << " card_dim_key" << dim_idx << ";\n";

    code << " std::unordered_map<" << struct_name << ","
         << "util::Bitset<" << std::to_string(guard.dim()->num_type().size())
         << ">," << struct_name << "::Hash," << struct_name
         << "::KeyEqual> card_stats" << dim_idx << ";\n";
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

  code << " db::UpsertStats stats;\n"
          "};\n";
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
          "__attribute__((__visibility__(\"default\")));\n"
          "extern \"C\" void* viya_upsert_context(const db::Table &table) {\n"
          " UpsertContext* ctx = new UpsertContext();\n";
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
  code << " return static_cast<void*>(ctx);\n"
          "}\n"
          "extern \"C\" db::SegmentBase* viya_segment_create() "
          "__attribute__((__visibility__(\"default\")));\n"
          "extern \"C\" db::SegmentBase* viya_segment_create() {\n"
          " return new Segment();\n"
          "}\n";

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
