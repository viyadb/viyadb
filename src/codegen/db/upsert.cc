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

#include "codegen/db/upsert.h"
#include "codegen/db/rollup.h"
#include "codegen/db/store.h"
#include "db/column.h"
#include "db/database.h"
#include "db/defs.h"
#include "db/table.h"
#include "input/loader_desc.h"

namespace viya {
namespace codegen {

namespace db = viya::db;

void ValueParser::Visit(const db::StrDimension *dimension) {
  auto dim_idx = std::to_string(dimension->index());
  code_ << " auto& value = values[" << tuple_idx_map_[value_idx_++] << "];\n";

  auto max_length = dimension->length();
  if (max_length != -1) {
    code_ << " if (UNLIKELY(value.length() > " << std::to_string(max_length)
          << ")) {\n";
    code_ << "  value.erase(" << std::to_string(max_length) << ");\n";
    code_ << " }\n";
  }

  code_ << " auto it = uctx->v2c" << dim_idx << "->find(value);\n";
  code_ << " if (LIKELY(it != uctx->v2c" << dim_idx << "->end())) {\n";
  code_ << "  upsert_tuple.d._" << dim_idx << " = it->second;\n";
  code_ << " } else {\n";
  code_ << "  uctx->dict" << dim_idx << "->lock().lock();\n";
  code_ << "  auto code = uctx->dict" << dim_idx << "->c2v().size();\n";

  auto cardinality = dimension->cardinality();
  bool check_cardinality = cardinality < UINT64_MAX - 1;
  if (check_cardinality) {
    code_ << "  if(LIKELY(code <= " << cardinality << "U)) {\n";
  }
  code_ << "  upsert_tuple.d._" << dim_idx << " = code;\n";
  code_ << "  uctx->v2c" << dim_idx << "->emplace(value, code);\n";
  code_ << "  uctx->dict" << dim_idx << "->c2v().emplace_back(value);\n";
  if (check_cardinality) {
    code_ << "  } else {\n";
    code_ << "   upsert_tuple.d._" << dim_idx << " = 0;\n";
    code_ << "  }\n";
  }

  code_ << "  uctx->dict" << dim_idx << "->lock().unlock();\n";
  code_ << " }\n";
}

void ValueParser::Visit(const db::NumDimension *dimension) {
  code_ << " upsert_tuple.d._" << std::to_string(dimension->index()) << " = "
        << dimension->num_type().cpp_parse_fn() << "(values["
        << tuple_idx_map_[value_idx_++] << "]);\n";
}

void ValueParser::Visit(const db::TimeDimension *dimension) {
  auto dim_idx = std::to_string(dimension->index());

  auto format = dimension->format();
  bool is_posix_ts = (format == "posix");
  bool is_milli_ts = (format == "millis");
  bool is_micro_ts = (format == "micros");

  bool is_num_input =
      format.empty() || is_posix_ts || is_milli_ts || is_micro_ts;
  if (is_num_input) {
    code_ << " {\n";
    code_ << "  uint64_t ts_val = std::stoull(values["
          << tuple_idx_map_[value_idx_] << "]);\n";
    if (dimension->micro_precision()) {
      if (is_posix_ts) {
        code_ << "  ts_val *= 1000000L;\n";
      } else if (is_milli_ts) {
        code_ << "  ts_val *= 1000L;\n";
      }
    } else {
      if (is_milli_ts) {
        code_ << "  ts_val /= 1000L;\n";
      } else if (is_micro_ts) {
        code_ << "  ts_val /= 1000000L;\n";
      }
    }
    code_ << "  upsert_tuple.d._" << dim_idx << " = ts_val;\n";
    code_ << " }\n";
    if (!dimension->rollup_rules().empty() ||
        !dimension->granularity().empty()) {
      code_ << " uctx->time" << dim_idx << ".set_ts(upsert_tuple.d._" << dim_idx
            << ");\n";
    }
  } else {
    code_ << " uctx->time" << dim_idx << ".parse(\"" << format << "\", values["
          << tuple_idx_map_[value_idx_] << "]);\n";
    if (!dimension->rollup_rules().empty()) {
      code_ << " upsert_tuple.d._" << dim_idx << " = uctx->time" << dim_idx
            << ".get_ts();\n";
    }
  }

  if (!dimension->rollup_rules().empty()) {
    TimestampRollup ts_rollup(dimension, "upsert_tuple.d._" + dim_idx,
                              "uctx->");
    code_ << ts_rollup.GenerateCode();
  } else if (!dimension->granularity().empty()) {
    code_ << " uctx->time" << dim_idx << ".trunc<static_cast<util::TimeUnit>("
          << static_cast<int>(dimension->granularity().time_unit())
          << ")>();\n";
  }

  if (!is_num_input || !dimension->rollup_rules().empty() ||
      !dimension->granularity().empty()) {
    code_ << " upsert_tuple.d._" << dim_idx << " = uctx->time" << dim_idx
          << ".get_ts();\n";
  }

  ++value_idx_;
}

void ValueParser::Visit(const db::BoolDimension *dimension) {
  code_ << " upsert_tuple.d._" << std::to_string(dimension->index())
        << " = values[" << tuple_idx_map_[value_idx_++] << "] == \"true\";\n";
}

void ValueParser::Visit(const db::ValueMetric *metric) {
  auto metric_idx = std::to_string(metric->index());
  code_ << " upsert_tuple.m._" << metric_idx << " = ";
  if (metric->agg_type() == db::Metric::AggregationType::COUNT) {
    code_ << "1";
  } else {
    code_ << metric->num_type().cpp_parse_fn() << "(values["
          << tuple_idx_map_[value_idx_++] << "])";
  }
  code_ << ";\n";
}

void ValueParser::Visit(const db::BitsetMetric *metric) {
  auto metric_idx = std::to_string(metric->index());
  code_ << " upsert_tuple.m._" << metric_idx << " = "
        << metric->num_type().cpp_parse_fn() << "(values["
        << tuple_idx_map_[value_idx_++] << "]);\n";
}

UpsertGenerator::UpsertGenerator(const input::LoaderDesc &desc)
    : FunctionGenerator(
          const_cast<db::Database &>(desc.table().database()).compiler()),
      desc_(desc) {}

Code UpsertGenerator::LoaderContextCode() const {
  Code code;
  code << "struct LoaderContext {\n";
  code << " db::Table* table;\n";

  if (desc_.has_partition_filter()) {
    code << " bool partition_values["
         << desc_.partition_filter().total_partitions() << "] = {false};\n";
  }

  code << "};\n";
  return code;
}

Code UpsertGenerator::OptimizeCode() const {
  Code code;
  auto &table = desc_.table();
  bool has_bitset_metric = std::any_of(
      table.metrics().cbegin(), table.metrics().cend(),
      [](const db::Metric *metric) {
        return metric->agg_type() == db::Metric::AggregationType::BITSET;
      });
  if (!has_bitset_metric) {
    code << " for (auto* s : lctx->table->store()->segments_copy()) {\n";
    code << "  auto segment_size = s->size();\n";
    code << "  auto metrics = static_cast<Segment*>(s)->m;\n";
    code << "  for (size_t tuple_idx = 0; tuple_idx < segment_size; "
            "++tuple_idx) {\n";
    for (auto *metric : table.metrics()) {
      if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
        code << "   metrics._" << std::to_string(metric->index())
             << "[tuple_idx].optimize();\n";
      }
    }
    code << "  }\n";
    code << " }\n";
  }
  return code;
}

Code UpsertGenerator::SetupFunctionCode() const {
  Code code;
  auto &table = desc_.table();
  auto &cardinality_guards = table.cardinality_guards();

  code.AddHeaders({"vector", "string", "db/store.h", "db/table.h",
                   "db/dictionary.h", "util/likely.h", "input/loader_desc.h"});

  if (!cardinality_guards.empty()) {
    code.AddHeaders({"util/bitset.h"});
  }
  if (desc_.has_partition_filter()) {
    code.AddHeaders({"util/crc32.h"});
  }

  code.AddNamespaces({"input = viya::input", "util = viya::util"});

  StoreDefs store_defs(table);
  code << store_defs.GenerateCode();

  UpsertContextDefs uc_defs(table);
  code << uc_defs.GenerateCode();

  code << LoaderContextCode();

  code << "extern \"C\" void* viya_upsert_setup(const input::LoaderDesc& desc) "
          "__attribute__((__visibility__(\"default\")));\n";
  code << "extern \"C\" void* viya_upsert_setup(const input::LoaderDesc& desc) "
          "{\n";
  code << " LoaderContext* lctx = new LoaderContext();\n";
  code << " lctx->table = const_cast<db::Table*>(&(desc.table()));\n";
  if (desc_.has_partition_filter()) {
    code << " for(auto& v : desc.partition_filter().values()) "
            "lctx->partition_values[v] = true;\n";
  }
  code << " return static_cast<void*>(lctx);\n";
  code << "}\n";

  code << "extern \"C\" void viya_upsert_before(void* lctx_ptr) "
          "__attribute__((__visibility__(\"default\")));\n";
  code << "extern \"C\" void viya_upsert_before(void* lctx_ptr) {\n";
  code << " LoaderContext* lctx = static_cast<LoaderContext*>(lctx_ptr);\n";
  code << " UpsertContext* uctx = "
          "static_cast<UpsertContext*>(lctx->table->upsert_ctx());\n";
  code << " uctx->Reset();\n";
  code << "}\n";

  code << "extern \"C\" db::UpsertStats viya_upsert_after(void* lctx_ptr) "
          "__attribute__((__visibility__(\"default\")));\n";
  code << "extern \"C\" db::UpsertStats viya_upsert_after(void* lctx_ptr) {\n";
  code << " LoaderContext* lctx = static_cast<LoaderContext*>(lctx_ptr);\n";
  code << " UpsertContext* uctx = "
          "static_cast<UpsertContext*>(lctx->table->upsert_ctx());\n";

  if (AddOptimize()) {
    code << OptimizeCode();
    code << " uctx->Optimize();\n";
  }

  code << " return uctx->stats;\n";
  code << "}\n";

  return code;
}

bool UpsertGenerator::AddOptimize() const {
  auto &table = desc_.table();
  bool has_bitset_metric = std::any_of(
      table.metrics().cbegin(), table.metrics().cend(),
      [](const db::Metric *metric) {
        return metric->agg_type() == db::Metric::AggregationType::BITSET;
      });
  return has_bitset_metric || !table.cardinality_guards().empty();
}

Code UpsertGenerator::CardinalityProtection() const {
  Code code;
  auto &table = desc_.table();

  for (auto &guard : table.cardinality_guards()) {
    code << "{\n";
    auto dim_idx = std::to_string(guard.dim()->index());
    for (auto per_dim : guard.dimensions()) {
      auto per_dim_idx = std::to_string(per_dim->index());
      code << " uctx->card_dim_key" << dim_idx << "._" << per_dim_idx
           << " = upsert_tuple.d._" << per_dim_idx << ";\n";
    }
    code << " auto it = uctx->card_stats" << dim_idx
         << ".find(uctx->card_dim_key" << dim_idx << ");\n";
    code << " if (it == uctx->card_stats" << dim_idx << ".end()) {\n";
    code << "  util::Bitset<" << std::to_string(guard.dim()->num_type().size())
         << "> bitset;\n";
    code << "  bitset.add(upsert_tuple.d._" << dim_idx << ");\n";
    code << "  uctx->card_stats" << dim_idx << ".emplace(uctx->card_dim_key"
         << dim_idx << ", std::move(bitset));\n";
    code << " } else {\n";
    code << "  auto& bitset = it->second;\n";
    code << "  if (UNLIKELY(bitset.cardinality() >= " << guard.limit()
         << ")) {\n";
    code << "   if (UNLIKELY(!bitset.contains(upsert_tuple.d._" << dim_idx
         << "))) {\n";
    code << "    upsert_tuple.d._" << dim_idx << " = 0;\n";
    code << "   }\n";
    code << "  } else {\n";
    code << "   bitset.add(upsert_tuple.d._" << dim_idx << ");\n";
    code << "  }\n";
    code << " }\n";
    code << "}\n";
  }
  return code;
}

Code UpsertGenerator::PartitionFilter() const {
  Code code;
  if (desc_.has_partition_filter()) {
    auto &part_filter = desc_.partition_filter();
    auto &tuple_idx_map = desc_.tuple_idx_map();
    auto &table = desc_.table();
    code << "{\n";
    code << " uint32_t hash = 0;\n";
    for (auto &key_col : part_filter.columns()) {
      auto dim = table.dimension(key_col);
      code << " {\n";
      code << "  auto& value = values[" << tuple_idx_map[dim->index()]
           << "];\n";
      code << "  hash = util::crc32(hash, value);\n";
      code << " }\n";
    }
    code << " if(!lctx->partition_values[hash % "
         << std::to_string(part_filter.total_partitions()) << "]) return;\n";
    code << "}\n";
  }
  return code;
}

Code UpsertGenerator::GenerateCode() const {
  Code code;
  auto &table = desc_.table();

  code << SetupFunctionCode();

  code << "extern \"C\" void viya_upsert_do(void* lctx_ptr, "
          "std::vector<std::string>& values) "
          "__attribute__((__visibility__(\"default\")));\n";
  code << "extern \"C\" void viya_upsert_do(void* lctx_ptr, "
          "std::vector<std::string>& values) {\n";

  code << " LoaderContext* lctx = static_cast<LoaderContext*>(lctx_ptr);\n";
  code << " UpsertContext* uctx = "
          "static_cast<UpsertContext*>(lctx->table->upsert_ctx());\n";
  code << " auto& upsert_tuple = uctx->upsert_tuple;\n";

  code << PartitionFilter();

  size_t value_idx = 0;
  ValueParser value_parser(code, desc_.tuple_idx_map(), value_idx);

  for (auto *dimension : table.dimensions()) {
    code << "{\n";
    dimension->Accept(value_parser);
    code << "}\n";
  }

  bool has_count_metric = false;
  bool has_avg_metric = false;
  for (auto *metric : table.metrics()) {
    if (metric->agg_type() == db::Metric::AggregationType::AVG) {
      has_avg_metric = true;
    } else if (metric->agg_type() == db::Metric::AggregationType::COUNT) {
      has_count_metric = true;
    }
    metric->Accept(value_parser);
  }
  if (has_avg_metric && !has_count_metric) {
    code << " upsert_tuple.m._count = 1;\n";
  }

  code << CardinalityProtection();

  code << " auto* store = lctx->table->store();\n";
  code << " auto& segments = store->segments();\n";
  code << " auto offset_it = uctx->tuple_offsets.find(upsert_tuple.d);\n";
  code << " if (offset_it != uctx->tuple_offsets.end()) {\n";
  auto segment_size = std::to_string(table.segment_size());
  code << "  size_t global_idx = offset_it->second;\n";
  code << "  size_t segment_idx = global_idx / " << segment_size << ";\n";
  code << "  size_t tuple_idx = global_idx % " << segment_size << ";\n";
  code << "  static_cast<Segment*>(segments[segment_idx])";
  code << "->m.Update(upsert_tuple.m,tuple_idx);\n";

  if (AddOptimize()) {
    code << "  if (--uctx->updates_before_optimize == 0) {\n";
    code << OptimizeCode();
    code << "   uctx->Optimize();\n";
    code << "  }\n";
  }

  code << " } else {\n";
  code << "  auto last_segment = static_cast<Segment*>(store->last());\n";
  code << "  size_t segment_idx = segments.size() - 1;\n";
  code << "  size_t tuple_idx = last_segment->size();\n";
  code << "  last_segment->Insert(upsert_tuple);\n";
  code << "  last_segment->stats.Update(upsert_tuple);\n";
  code << "  uctx->stats.new_recs++;\n";
  code << "  uctx->tuple_offsets.emplace(upsert_tuple.d, segment_idx * "
       << segment_size << " + tuple_idx);\n";
  code << " }\n";

  // Empty temporary roaring bitsets:
  for (auto *metric : table.metrics()) {
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      auto metric_idx = std::to_string(metric->index());
      code << " upsert_tuple.m._" << metric_idx << " = uctx->empty_bitset"
           << metric_idx << ";\n";
    }
  }
  code << "}\n";
  return code;
}

UpsertSetupFn UpsertGenerator::SetupFunction() {
  return GenerateFunction<UpsertSetupFn>(std::string("viya_upsert_setup"));
}

BeforeUpsertFn UpsertGenerator::BeforeFunction() {
  return GenerateFunction<BeforeUpsertFn>(std::string("viya_upsert_before"));
}

AfterUpsertFn UpsertGenerator::AfterFunction() {
  return GenerateFunction<AfterUpsertFn>(std::string("viya_upsert_after"));
}

UpsertFn UpsertGenerator::Function() {
  return GenerateFunction<UpsertFn>(std::string("viya_upsert_do"));
}

} // namespace codegen
} // namespace viya
