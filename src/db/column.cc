#include <cstdint>
#include <stdexcept>
#include <ctime>
#include "db/column.h"
#include "db/table.h"
#include "db/dictionary.h"
#include "util/sanitize.h"

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

const AnyNum UIntType::Parse(const std::string& value) const {
  switch (size()) {
    case Size::_1: return AnyNum((uint8_t)std::stoul(value));
    case Size::_2: return AnyNum((uint16_t)std::stoul(value));
    case Size::_4: return AnyNum((uint32_t)std::stoul(value));
    case Size::_8: return AnyNum((uint64_t)std::stoull(value));
  }
  throw std::runtime_error("Unsupported type");
}

UIntType max_value_to_uint_type(size_t max_value) {
  if (max_value - 1 < UINT8_MAX)  return UIntType(NumericType::Size::_1);
  if (max_value - 1 < UINT16_MAX) return UIntType(NumericType::Size::_2);
  if (max_value - 1 < UINT32_MAX) return UIntType(NumericType::Size::_4);
  return UIntType(NumericType::Size::_8);
}

Column::Column(const util::Config& config, Type type, size_t index):type_(type),index_(index) {
  name_ = config.str("name");
  util::check_legal_string("Column name", name_);
}

StrDimension::StrDimension(const util::Config& config, size_t index, Dictionaries& dicts)
  :Dimension(config, index, Dimension::DimType::STRING, max_value_to_uint_type(
      cardinality_ = config.num("cardinality", UINT32_MAX))),
  length_(config.num("length", -1))
{
  dict_ = dicts.GetOrCreate(name(), num_type());
}

void StrDimension::Accept(ColumnVisitor& visitor) const {
  visitor.Visit(this);
}

NumDimension::NumDimension(const util::Config& config, size_t index)
  :Dimension(config, index, Dimension::DimType::NUMERIC, max_value_to_uint_type(
      config.num("max", UINT32_MAX))) {
}

void NumDimension::Accept(ColumnVisitor& visitor) const {
  visitor.Visit(this);
}

TimeDimension::TimeDimension(const util::Config& config, size_t index, bool micro_precision)
  :Dimension(config, index, Dimension::DimType::TIME,
             UIntType(micro_precision ? NumericType::Size::_8 : NumericType::Size::_4)),
  micro_precision_(micro_precision) {

  if (config.exists("format")) {
    format_ = config.str("format");
    util::check_legal_string("Time format", format_);
  }

  if (config.exists("granularity")) {
    granularity_ = Granularity(config.str("granularity"));
  }
  else if (config.exists("rollup_rules")) {
    for (const util::Config& rollup_conf : config.sublist("rollup_rules")) {
      rollup_rules_.emplace_back(rollup_conf);
    }
    std::sort(
      rollup_rules_.begin(), rollup_rules_.end(), [](const RollupRule& r1, const RollupRule& r2) -> bool { 
        return r1.after() > r2.after();
    });
  }
}

void TimeDimension::Accept(ColumnVisitor& visitor) const {
  visitor.Visit(this);
}

BoolDimension::BoolDimension(const util::Config& config, size_t index)
  :Dimension(config, index, Dimension::DimType::BOOLEAN, NumericType::Size::_1) {
}

void BoolDimension::Accept(ColumnVisitor& visitor) const {
  visitor.Visit(this);
}

Metric::AggregationType parse_agg_type(const util::Config& config) {
  std::string type = config.str("type");
  auto sep_pos = type.find("_");
  if (sep_pos != std::string::npos) {
    auto agg_type = type.substr(sep_pos + 1);
    if (agg_type == "sum") { return Metric::AggregationType::SUM; }
    if (agg_type == "max") { return Metric::AggregationType::MAX; }
    if (agg_type == "min") { return Metric::AggregationType::MIN; }
  }
  if (type == "count")  { return Metric::AggregationType::COUNT; }
  if (type == "bitset") { return Metric::AggregationType::BITSET; }
  throw std::invalid_argument("Unsupported metric type: " + type);
}

MetricType::Type parse_metric_type(const std::string& type_name) {
  if (type_name == "int")    { return MetricType::Type::INT; }
  if (type_name == "uint")   { return MetricType::Type::UINT; }
  if (type_name == "long")   { return MetricType::Type::LONG; }
  if (type_name == "ulong")  { return MetricType::Type::ULONG; }
  if (type_name == "double") { return MetricType::Type::DOUBLE; }
  throw std::invalid_argument("Unsupported metric type: " + type_name);
}

MetricType::MetricType(const Type type):
  NumericType((type == Type::INT || type == Type::UINT) ? NumericType::Size::_4 : NumericType::Size::_8),
  type_(type)
{
}

MetricType::MetricType(const std::string& type_name)
  :MetricType(parse_metric_type(type_name)) {
}

const std::string MetricType::cpp_type() const {
  if (type_ == Type::INT)    { return "int32_t"; }
  if (type_ == Type::UINT)   { return "uint32_t"; }
  if (type_ == Type::LONG)   { return "int64_t"; }
  if (type_ == Type::ULONG)  { return "uint64_t"; }
  if (type_ == Type::DOUBLE) { return "double"; }
  throw std::runtime_error("Unsupported type");
}

const std::string MetricType::cpp_max_value() const {
  if (type_ == Type::INT)    { return "INT32_MAX"; }
  if (type_ == Type::UINT)   { return "UINT32_MAX"; }
  if (type_ == Type::LONG)   { return "INT64_MAX"; }
  if (type_ == Type::ULONG)  { return "UINT64_MAX"; }
  if (type_ == Type::DOUBLE) { return "DBL_MAX"; }
  throw std::runtime_error("Unsupported type");
}

const std::string MetricType::cpp_min_value() const {
  if (type_ == Type::INT)    { return "INT32_MIN"; }
  if (type_ == Type::UINT)   { return "UINT32_MIN"; }
  if (type_ == Type::LONG)   { return "INT64_MIN"; }
  if (type_ == Type::ULONG)  { return "UINT64_MIN"; }
  if (type_ == Type::DOUBLE) { return "DBL_MIN"; }
  throw std::runtime_error("Unsupported type");
}

const std::string MetricType::cpp_parse_fn() const {
  switch (type_) {
    case Type::INT:    return "std::stol";
    case Type::UINT:   return "std::stoul";
    case Type::LONG:   return "std::stoll";
    case Type::ULONG:  return "std::stoull";
    case Type::DOUBLE: return "std::stod";
  }
  throw std::runtime_error("Unsupported type");
}

const AnyNum MetricType::Parse(const std::string& value) const {
  switch (type_) {
    case Type::INT:    return AnyNum((int32_t)std::stol(value));
    case Type::UINT:   return AnyNum((uint32_t)std::stoul(value));
    case Type::LONG:   return AnyNum((int64_t)std::stoll(value));
    case Type::ULONG:  return AnyNum((uint64_t)std::stoull(value));
    case Type::DOUBLE: return AnyNum(std::stod(value));
  }
  throw std::runtime_error("Unsupported type");
}

MetricType parse_value_metric_type(const util::Config& config, Metric::AggregationType agg_type) {
  std::string type = config.str("type");
  if (agg_type == Metric::AggregationType::COUNT) {
    auto count_type = max_value_to_uint_type(config.num("max", UINT32_MAX));
    if (count_type.size() == NumericType::Size::_8) {
      return MetricType(MetricType::Type::ULONG);
    }
    return MetricType(MetricType::Type::UINT);
  }
  return MetricType(type.substr(0, type.find("_")));
}

ValueMetric::ValueMetric(const util::Config& config, size_t index)
  :Metric(config, index, parse_agg_type(config)),
  num_type_(parse_value_metric_type(config, agg_type()))
{
}

void ValueMetric::Accept(ColumnVisitor& visitor) const {
  visitor.Visit(this);
}

BitsetMetric::BitsetMetric(const util::Config& config, size_t index)
  :Metric(config, index, AggregationType::BITSET),
  num_type_(max_value_to_uint_type(config.num("max", UINT32_MAX)))
{
}

void BitsetMetric::Accept(ColumnVisitor& visitor) const {
  visitor.Visit(this);
}

}}

