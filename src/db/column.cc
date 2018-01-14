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

#include "db/column.h"
#include "db/dictionary.h"
#include "util/sanitize.h"
#include <cstdint>
#include <ctime>
#include <glog/logging.h>
#include <stdexcept>

namespace viya {
namespace db {

const std::string UIntType::cpp_parse_fn() const {
  switch (size()) {
  case Size::_1:
  case Size::_2:
  case Size::_4:
    return "std::stoul";
  case Size::_8:
    return "std::stoull";
  }
  throw std::runtime_error("Unsupported type");
}

const AnyNum UIntType::Parse(const std::string &value) const {
  switch (size()) {
  case Size::_1:
    return AnyNum((uint8_t)std::stoul(value));
  case Size::_2:
    return AnyNum((uint16_t)std::stoul(value));
  case Size::_4:
    return AnyNum((uint32_t)std::stoul(value));
  case Size::_8:
    return AnyNum((uint64_t)std::stoull(value));
  }
  throw std::runtime_error("Unsupported type");
}

UIntType max_value_to_uint_type(size_t max_value) {
  if (max_value - 1 < UINT8_MAX)
    return UIntType(BaseNumType::Size::_1);
  if (max_value - 1 < UINT16_MAX)
    return UIntType(BaseNumType::Size::_2);
  if (max_value - 1 < UINT32_MAX)
    return UIntType(BaseNumType::Size::_4);
  return UIntType(BaseNumType::Size::_8);
}

NumericType::Type parse_metric_type(const std::string &type_name) {
  if (type_name == "byte") {
    return NumericType::Type::BYTE;
  }
  if (type_name == "ubyte") {
    return NumericType::Type::UBYTE;
  }
  if (type_name == "short") {
    return NumericType::Type::SHORT;
  }
  if (type_name == "ushort") {
    return NumericType::Type::USHORT;
  }
  if (type_name == "int") {
    return NumericType::Type::INT;
  }
  if (type_name == "uint") {
    return NumericType::Type::UINT;
  }
  if (type_name == "long") {
    return NumericType::Type::LONG;
  }
  if (type_name == "ulong") {
    return NumericType::Type::ULONG;
  }
  if (type_name == "float") {
    return NumericType::Type::FLOAT;
  }
  if (type_name == "double") {
    return NumericType::Type::DOUBLE;
  }
  throw std::invalid_argument("Unsupported metric type: " + type_name);
}

BaseNumType::Size numeric_type_to_size(NumericType::Type type) {
  switch (type) {
  case NumericType::Type::BYTE:
  case NumericType::Type::UBYTE:
    return BaseNumType::Size::_1;
  case NumericType::Type::SHORT:
  case NumericType::Type::USHORT:
    return BaseNumType::Size::_2;
  case NumericType::Type::INT:
  case NumericType::Type::UINT:
  case NumericType::Type::FLOAT:
    return BaseNumType::Size::_4;
  default:
    return BaseNumType::Size::_8;
  }
}

NumericType::NumericType(const Type type)
    : BaseNumType(numeric_type_to_size(type)), type_(type) {}

NumericType::NumericType(const std::string &type_name)
    : NumericType(parse_metric_type(type_name)) {}

const std::string NumericType::cpp_type() const {
  if (type_ == Type::BYTE) {
    return "int8_t";
  }
  if (type_ == Type::UBYTE) {
    return "uint8_t";
  }
  if (type_ == Type::SHORT) {
    return "int16_t";
  }
  if (type_ == Type::USHORT) {
    return "uint16_t";
  }
  if (type_ == Type::INT) {
    return "int32_t";
  }
  if (type_ == Type::UINT) {
    return "uint32_t";
  }
  if (type_ == Type::LONG) {
    return "int64_t";
  }
  if (type_ == Type::ULONG) {
    return "uint64_t";
  }
  if (type_ == Type::FLOAT) {
    return "float";
  }
  if (type_ == Type::DOUBLE) {
    return "double";
  }
  throw std::runtime_error("Unsupported type");
}

const std::string NumericType::cpp_max_value() const {
  if (type_ == Type::BYTE) {
    return "INT8_MAX";
  }
  if (type_ == Type::UBYTE) {
    return "UINT8_MAX";
  }
  if (type_ == Type::SHORT) {
    return "INT16_MAX";
  }
  if (type_ == Type::USHORT) {
    return "UINT16_MAX";
  }
  if (type_ == Type::INT) {
    return "INT32_MAX";
  }
  if (type_ == Type::UINT) {
    return "UINT32_MAX";
  }
  if (type_ == Type::LONG) {
    return "INT64_MAX";
  }
  if (type_ == Type::ULONG) {
    return "UINT64_MAX";
  }
  if (type_ == Type::FLOAT) {
    return "FLT_MAX";
  }
  if (type_ == Type::DOUBLE) {
    return "DBL_MAX";
  }
  throw std::runtime_error("Unsupported type");
}

const std::string NumericType::cpp_min_value() const {
  if (type_ == Type::BYTE) {
    return "INT8_MIN";
  }
  if (type_ == Type::UBYTE) {
    return "0U";
  }
  if (type_ == Type::SHORT) {
    return "INT16_MIN";
  }
  if (type_ == Type::USHORT) {
    return "0U";
  }
  if (type_ == Type::INT) {
    return "INT32_MIN";
  }
  if (type_ == Type::UINT) {
    return "0U";
  }
  if (type_ == Type::LONG) {
    return "INT64_MIN";
  }
  if (type_ == Type::ULONG) {
    return "0UL";
  }
  if (type_ == Type::FLOAT) {
    return "FLT_MIN";
  }
  if (type_ == Type::DOUBLE) {
    return "DBL_MIN";
  }
  throw std::runtime_error("Unsupported type");
}

const std::string NumericType::cpp_parse_fn() const {
  switch (type_) {
  case Type::BYTE:
    return "std::stoi";
  case Type::UBYTE:
    return "std::stoul";
  case Type::SHORT:
    return "std::stoi";
  case Type::USHORT:
    return "std::stoul";
  case Type::INT:
    return "std::stoi";
  case Type::UINT:
    return "std::stoul";
  case Type::LONG:
    return "std::stoll";
  case Type::ULONG:
    return "std::stoull";
  case Type::FLOAT:
    return "std::stof";
  case Type::DOUBLE:
    return "std::stod";
  }
  throw std::runtime_error("Unsupported type");
}

const AnyNum NumericType::Parse(const std::string &value) const {
  switch (type_) {
  case Type::BYTE:
    return AnyNum((int8_t)std::stoi(value));
  case Type::UBYTE:
    return AnyNum((uint8_t)std::stoul(value));
  case Type::SHORT:
    return AnyNum((int16_t)std::stoi(value));
  case Type::USHORT:
    return AnyNum((uint16_t)std::stoul(value));
  case Type::INT:
    return AnyNum((int32_t)std::stoi(value));
  case Type::UINT:
    return AnyNum((uint32_t)std::stoul(value));
  case Type::LONG:
    return AnyNum((int64_t)std::stoll(value));
  case Type::ULONG:
    return AnyNum((uint64_t)std::stoull(value));
  case Type::FLOAT:
    return AnyNum(std::stof(value));
  case Type::DOUBLE:
    return AnyNum(std::stod(value));
  }
  throw std::runtime_error("Unsupported type");
}

NumericType parse_value_metric_type(const util::Config &config,
                                    Metric::AggregationType agg_type) {
  std::string type = config.str("type");
  if (agg_type == Metric::AggregationType::COUNT) {
    auto count_type = max_value_to_uint_type(config.num("max", UINT32_MAX));
    if (count_type.size() == BaseNumType::Size::_8) {
      return NumericType(NumericType::Type::ULONG);
    }
    return NumericType(NumericType::Type::UINT);
  }
  return NumericType(type.substr(0, type.find("_")));
}

Column::Column(const util::Config &config, Type type, size_t index)
    : type_(type), index_(index), name_(config.str("name")) {

  util::check_legal_string("Column name", name_);
  if (config.exists("field")) {
    input_field_ = config.str("field");
  }
}

StrDimension::StrDimension(const util::Config &config, size_t index,
                           Dictionaries &dicts)
    : Dimension(config, index, Dimension::DimType::STRING),
      num_type_(max_value_to_uint_type(
          cardinality_ = config.num("cardinality", UINT32_MAX))),
      length_(config.num("length", -1)) {
  dict_ = dicts.GetOrCreate(name(), num_type());
}

void StrDimension::Accept(ColumnVisitor &visitor) const { visitor.Visit(this); }

NumericType parse_num_dimension_type(const util::Config &config) {
  std::string type = config.str("type");
  if (type == "numeric") {
    LOG(WARNING) << "numeric dimension type is deprecated. Please use specific "
                    "types: byte, ubyte, short, etc.";
    auto count_type = max_value_to_uint_type(config.num("max", UINT32_MAX));
    if (count_type.size() == BaseNumType::Size::_8) {
      return NumericType(NumericType::Type::ULONG);
    }
    return NumericType(NumericType::Type::UINT);
  }
  return NumericType(type);
}

NumDimension::NumDimension(const util::Config &config, size_t index)
    : Dimension(config, index, Dimension::DimType::NUMERIC),
      num_type_(parse_num_dimension_type(config)) {}

void NumDimension::Accept(ColumnVisitor &visitor) const { visitor.Visit(this); }

TimeDimension::TimeDimension(const util::Config &config, size_t index,
                             bool micro_precision)
    : Dimension(config, index, Dimension::DimType::TIME),
      num_type_(UIntType(micro_precision ? BaseNumType::Size::_8
                                         : BaseNumType::Size::_4)),
      micro_precision_(micro_precision) {

  if (config.exists("format")) {
    format_ = config.str("format");
    util::check_legal_string("Time format", format_);
  }

  if (config.exists("granularity")) {
    granularity_ = Granularity(config.str("granularity"));
  } else if (config.exists("rollup_rules")) {
    for (const util::Config &rollup_conf : config.sublist("rollup_rules")) {
      rollup_rules_.emplace_back(rollup_conf);
    }
    std::sort(rollup_rules_.begin(), rollup_rules_.end(),
              [](const RollupRule &r1, const RollupRule &r2) -> bool {
                return r1.after() > r2.after();
              });
  }
}

void TimeDimension::Accept(ColumnVisitor &visitor) const {
  visitor.Visit(this);
}

BoolDimension::BoolDimension(const util::Config &config, size_t index)
    : Dimension(config, index, Dimension::DimType::BOOLEAN),
      num_type_(BaseNumType::Size::_1) {}

void BoolDimension::Accept(ColumnVisitor &visitor) const {
  visitor.Visit(this);
}

Metric::AggregationType parse_agg_type(const util::Config &config) {
  std::string type = config.str("type");
  auto sep_pos = type.find("_");
  if (sep_pos != std::string::npos) {
    auto agg_type = type.substr(sep_pos + 1);
    if (agg_type == "sum") {
      return Metric::AggregationType::SUM;
    }
    if (agg_type == "max") {
      return Metric::AggregationType::MAX;
    }
    if (agg_type == "min") {
      return Metric::AggregationType::MIN;
    }
    if (agg_type == "avg") {
      return Metric::AggregationType::AVG;
    }
  }
  if (type == "count") {
    return Metric::AggregationType::COUNT;
  }
  if (type == "bitset") {
    return Metric::AggregationType::BITSET;
  }
  throw std::invalid_argument("Unsupported metric type: " + type);
}

ValueMetric::ValueMetric(const util::Config &config, size_t index)
    : Metric(config, index, parse_agg_type(config)),
      num_type_(parse_value_metric_type(config, agg_type())) {}

void ValueMetric::Accept(ColumnVisitor &visitor) const { visitor.Visit(this); }

BitsetMetric::BitsetMetric(const util::Config &config, size_t index)
    : Metric(config, index, AggregationType::BITSET),
      num_type_(max_value_to_uint_type(config.num("max", UINT32_MAX))) {}

void BitsetMetric::Accept(ColumnVisitor &visitor) const { visitor.Visit(this); }
} // namespace db
} // namespace viya
