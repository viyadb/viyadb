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

#include "codegen/query/filter.h"
#include "db/column.h"
#include "db/dictionary.h"
#include "db/table.h"
#include "util/macros.h"
#include <algorithm>
#include <cstring>
#include <ctime>

namespace viya {
namespace codegen {

class ArgsUnpacker : public query::FilterVisitor {
public:
  ArgsUnpacker(const db::Table &table, Code &code,
               const std::string &var_prefix)
      : table_(table), argidx_(0), code_(code), var_prefix_(var_prefix) {}

  DISALLOW_COPY_AND_MOVE(ArgsUnpacker);

  void Visit(const query::RelOpFilter *filter);
  void Visit(const query::InFilter *filter);
  void Visit(const query::CompositeFilter *filter);
  void Visit(const query::EmptyFilter *filter);

private:
  void UnpackArg(const db::Column *column);

private:
  const db::Table &table_;
  size_t argidx_;
  Code &code_;
  const std::string var_prefix_;
};

class ValueDecoder : public db::ColumnVisitor {
public:
  ValueDecoder(const std::string &value) : value_(value) {}

  void Visit(const db::StrDimension *dimension);
  void Visit(const db::NumDimension *dimension);
  void Visit(const db::TimeDimension *dimension);
  void Visit(const db::BoolDimension *dimension);
  void Visit(const db::ValueMetric *metric);
  void Visit(const db::BitsetMetric *metric);

  const db::AnyNum &decoded_value() const { return decoded_value_; }

private:
  const std::string &value_;
  db::AnyNum decoded_value_;
};

class ComparisonBuilder : public query::FilterVisitor {
public:
  ComparisonBuilder(const db::Table &table, Code &code,
                    const std::string &var_prefix = "farg")
      : table_(table), argidx_(0), code_(code), var_prefix_(var_prefix) {}

  void Visit(const query::RelOpFilter *filter);
  void Visit(const query::InFilter *filter);
  void Visit(const query::CompositeFilter *filter);
  void Visit(const query::EmptyFilter *filter);

protected:
  const db::Table &table_;
  size_t argidx_;
  Code &code_;
  const std::string var_prefix_;
};

class SegmentSkipBuilder : public ComparisonBuilder {
public:
  SegmentSkipBuilder(const db::Table &table, Code &code)
      : ComparisonBuilder(table, code) {}

  void Visit(const query::RelOpFilter *filter);
  void Visit(const query::InFilter *filter);
};

void FilterArgsPacker::Visit(const query::RelOpFilter *filter) {
  const auto column = table_.column(filter->column());
  ValueDecoder value_decoder(filter->value());
  column->Accept(value_decoder);
  args_.push_back(value_decoder.decoded_value());
}

void FilterArgsPacker::Visit(const query::InFilter *filter) {
  const auto column = table_.column(filter->column());
  auto &values = filter->values();
  for (size_t i = 0; i < values.size(); ++i) {
    ValueDecoder value_decoder(values[i]);
    column->Accept(value_decoder);
    args_.push_back(value_decoder.decoded_value());
  }
}

void FilterArgsPacker::Visit(const query::CompositeFilter *filter) {
  for (auto f : filter->filters()) {
    f->Accept(*this);
  }
}

void FilterArgsPacker::Visit(const query::EmptyFilter *filter
                             __attribute__((unused))) {}

void ArgsUnpacker::UnpackArg(const db::Column *column) {
  auto type = column->num_type().cpp_type();
  auto arg_idx = std::to_string(argidx_);
  code_ << type << " " << var_prefix_ << arg_idx << " = " << var_prefix_ << "s["
        << arg_idx << "].get_" << type << "();\n";
  ++argidx_;
}

void ArgsUnpacker::Visit(const query::RelOpFilter *filter) {
  UnpackArg(table_.column(filter->column()));
}

void ArgsUnpacker::Visit(const query::InFilter *filter) {
  const auto column = table_.column(filter->column());
  for (size_t i = 0; i < filter->values().size(); ++i) {
    UnpackArg(column);
  }
}

void ArgsUnpacker::Visit(const query::CompositeFilter *filter) {
  for (auto f : filter->filters()) {
    f->Accept(*this);
  }
}

void ArgsUnpacker::Visit(const query::EmptyFilter *filter
                         __attribute__((unused))) {}

void ValueDecoder::Visit(const db::StrDimension *dimension) {
  decoded_value_ = dimension->dict()->Decode(value_);
}

void ValueDecoder::Visit(const db::NumDimension *dimension) {
  decoded_value_ = dimension->num_type().Parse(value_);
}

void ValueDecoder::Visit(const db::TimeDimension *dimension) {
  if (std::all_of(value_.begin(), value_.end(), ::isdigit)) {
    decoded_value_ = dimension->num_type().Parse(value_);
  } else {
    uint32_t multiplier = dimension->micro_precision() ? 1000000L : 1L;
    uint64_t ts = 0;
    std::tm tm;

    const char *r = strptime(value_.c_str(), "%Y-%m-%d %T", &tm);
    if (r != nullptr && *r == '\0') {
      ts = timegm(&tm) * multiplier;
    } else {
      if (r != nullptr && dimension->micro_precision() && *r == '.') {
        ts = timegm(&tm) * multiplier + std::stoul(r);
      } else {
        std::memset(&tm, 0, sizeof(tm));
        r = strptime(value_.c_str(), "%Y-%m-%d", &tm);
        if (r != nullptr && *r == '\0') {
          ts = timegm(&tm) * multiplier;
        }
      }
    }
    if (ts > 0) {
      decoded_value_ = dimension->micro_precision() ? db::AnyNum(ts)
                                                    : db::AnyNum((uint32_t)ts);
    } else {
      throw std::invalid_argument("Unrecognized time format: " + value_);
    }
  }
}

void ValueDecoder::Visit(const db::BoolDimension *dimension
                         __attribute__((unused))) {
  decoded_value_ = value_ == "true";
}

void ValueDecoder::Visit(const db::ValueMetric *metric) {
  decoded_value_ = metric->num_type().Parse(value_);
}

void ValueDecoder::Visit(const db::BitsetMetric *metric) {
  decoded_value_ = metric->num_type().Parse(value_);
}

void ComparisonBuilder::Visit(const query::RelOpFilter *filter) {
  const auto column = table_.column(filter->column());
  if (column->type() == db::Column::Type::DIMENSION) {
    code_ << "(tuple_dims._" << std::to_string(column->index())
          << filter->opstr() << var_prefix_ << std::to_string(argidx_++) << ")";
  } else {
    code_ << "(tuple_metrics._" << std::to_string(column->index());
    auto metric = static_cast<const db::Metric *>(column);
    if (metric->agg_type() == db::Metric::AggregationType::BITSET) {
      code_ << ".cardinality()";
    }
    code_ << filter->opstr() << var_prefix_ << std::to_string(argidx_++) << ")";
  }
}

void ComparisonBuilder::Visit(const query::InFilter *filter) {
  const auto column = table_.column(filter->column());
  auto dim_idx = std::to_string(column->index());
  code_ << "(";
  bool is_dim = column->type() == db::Column::Type::DIMENSION;
  auto struct_name = is_dim ? "tuple_dims" : "tuple_metrics";
  for (size_t i = 0; i < filter->values().size(); ++i) {
    if (i > 0) {
      code_ << (filter->equal() ? "|" : "&");
    }
    code_ << "(" << struct_name << "._" << dim_idx;
    if (!is_dim &&
        static_cast<const db::Metric *>(column)->agg_type() ==
            db::Metric::AggregationType::BITSET) {
      code_ << ".cardinality()";
    }
    code_ << (filter->equal() ? "==" : "!=") << var_prefix_
          << std::to_string(argidx_++) << ")";
  }
  code_ << ")";
}

void ComparisonBuilder::Visit(const query::CompositeFilter *filter) {
  auto filters = filter->filters();
  auto filters_num = filters.size();
  std::string op =
      filter->op() == query::CompositeFilter::Operator::AND ? " & " : " | ";
  code_ << "(";
  for (size_t i = 0; i < filters_num; ++i) {
    if (i > 0) {
      code_ << op;
    }
    filters[i]->Accept(*this);
  }
  code_ << ")";
}

void ComparisonBuilder::Visit(const query::EmptyFilter *filter
                              __attribute__((unused))) {
  code_ << "true";
}

void SegmentSkipBuilder::Visit(const query::RelOpFilter *filter) {
  bool applied = false;
  auto arg_idx = std::to_string(argidx_++);
  const auto column = table_.column(filter->column());

  if (column->type() == db::Column::Type::DIMENSION) {
    auto dim = static_cast<const db::Dimension *>(column);

    if (dim->dim_type() == db::Dimension::DimType::NUMERIC ||
        dim->dim_type() == db::Dimension::DimType::TIME) {

      auto dim_idx = std::to_string(dim->index());
      applied = true;
      switch (filter->op()) {
      case query::RelOpFilter::Operator::EQUAL:
        code_ << "((segment->stats.dmin" << dim_idx << "<=farg" << arg_idx
              << ") & "
              << "(segment->stats.dmax" << dim_idx << ">=farg" << arg_idx
              << "))";
        break;
      case query::RelOpFilter::Operator::LESS:
      case query::RelOpFilter::Operator::LESS_EQUAL:
        code_ << "(segment->stats.dmin" << dim_idx << "<=farg" << arg_idx
              << ")";
        break;
      case query::RelOpFilter::Operator::GREATER:
      case query::RelOpFilter::Operator::GREATER_EQUAL:
        code_ << "(segment->stats.dmax" << dim_idx << ">=farg" << arg_idx
              << ")";
        break;
      default:
        applied = false;
      }
    }
  }
  if (!applied) {
    code_ << "1";
  }
}

void SegmentSkipBuilder::Visit(const query::InFilter *filter) {
  bool applied = false;
  const auto column = table_.column(filter->column());

  if (column->type() == db::Column::Type::DIMENSION) {
    auto dim = static_cast<const db::Dimension *>(column);

    if (dim->dim_type() == db::Dimension::DimType::NUMERIC ||
        dim->dim_type() == db::Dimension::DimType::TIME) {

      auto dim_idx = column->index();
      code_ << "(";
      for (size_t i = 0; i < filter->values().size(); ++i) {
        auto arg_idx = std::to_string(argidx_++);
        if (i > 0) {
          code_ << " | ";
        }
        code_ << "((segment->stats.dmin" << dim_idx << "<=farg" << arg_idx
              << ") & "
              << "(segment->stats.dmax" << dim_idx << ">=farg" << arg_idx
              << "))";
      }
      code_ << ")";
      applied = true;
    }
  }
  if (!applied) {
    for (size_t i = 0; i < filter->values().size(); ++i) {
      ++argidx_;
    }
    code_ << "1";
  }
}

Code FilterArgsUnpack::GenerateCode() const {
  Code code;
  ArgsUnpacker b(table_, code, var_prefix_);
  filter_->Accept(b);
  return code;
}

Code SegmentSkip::GenerateCode() const {
  Code code;
  SegmentSkipBuilder b(table_, code);
  filter_->Accept(b);
  return code;
}

Code FilterComparison::GenerateCode() const {
  Code code;
  ComparisonBuilder b(table_, code, var_prefix_);
  filter_->Accept(b);
  return code;
}
} // namespace codegen
} // namespace viya
