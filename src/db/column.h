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

#ifndef VIYA_DB_COLUMN_H_
#define VIYA_DB_COLUMN_H_

#include "db/rollup.h"
#include "util/config.h"
#include <array>
#include <string>

namespace viya {
namespace db {

namespace util = viya::util;

class Dictionaries;
class DimensionDict;

class BaseNumType {
public:
  // size in bytes:
  enum Size { _1 = 1, _2 = 2, _4 = 4, _8 = 8 };

  BaseNumType(Size size) : size_(size) {}

  Size size() const { return size_; }
  virtual const std::string cpp_parse_fn() const = 0;
  virtual const std::string cpp_type() const = 0;
  virtual const std::string cpp_max_value() const = 0;
  virtual const std::string cpp_min_value() const = 0;
  virtual const class AnyNum Parse(const std::string &value) const = 0;

private:
  Size size_;
};

class UIntType : public BaseNumType {
public:
  UIntType(Size size) : BaseNumType(size) {}

  const std::string cpp_parse_fn() const;
  const std::string cpp_type() const {
    return "uint" + std::to_string(size() * 8) + "_t";
  }
  const std::string cpp_max_value() const {
    return "UINT" + std::to_string(size() * 8) + "_MAX";
  }
  const std::string cpp_min_value() const { return "0"; }

  const class AnyNum Parse(const std::string &value) const;
};

class NumericType : public BaseNumType {
public:
  enum Type {
    BYTE,
    UBYTE,
    SHORT,
    USHORT,
    INT,
    UINT,
    LONG,
    ULONG,
    FLOAT,
    DOUBLE
  };

  NumericType(const Type type);
  NumericType(const std::string &type_name);

  const std::string cpp_parse_fn() const;
  const std::string cpp_type() const;
  const std::string cpp_max_value() const;
  const std::string cpp_min_value() const;
  Type type() const { return type_; }

  const AnyNum Parse(const std::string &value) const;

private:
  Type type_;
};

class AnyNum {
public:
  AnyNum() {}
  AnyNum(uint8_t num) { *reinterpret_cast<uint8_t *>(s_.data()) = num; }
  AnyNum(uint16_t num) { *reinterpret_cast<uint16_t *>(s_.data()) = num; }
  AnyNum(uint32_t num) { *reinterpret_cast<uint32_t *>(s_.data()) = num; }
  AnyNum(uint64_t num) { *reinterpret_cast<uint64_t *>(s_.data()) = num; }
  AnyNum(int32_t num) { *reinterpret_cast<int32_t *>(s_.data()) = num; }
  AnyNum(int64_t num) { *reinterpret_cast<int64_t *>(s_.data()) = num; }
  AnyNum(float num) { *reinterpret_cast<float *>(s_.data()) = num; }
  AnyNum(double num) { *reinterpret_cast<double *>(s_.data()) = num; }

  uint8_t get_uint8_t() { return *reinterpret_cast<uint8_t *>(s_.data()); }
  uint16_t get_uint16_t() { return *reinterpret_cast<uint16_t *>(s_.data()); }
  uint32_t get_uint32_t() { return *reinterpret_cast<uint32_t *>(s_.data()); }
  uint64_t get_uint64_t() { return *reinterpret_cast<uint64_t *>(s_.data()); }
  int32_t get_int32_t() { return *reinterpret_cast<int32_t *>(s_.data()); }
  int64_t get_int64_t() { return *reinterpret_cast<int64_t *>(s_.data()); }
  double get_float() { return *reinterpret_cast<float *>(s_.data()); }
  double get_double() { return *reinterpret_cast<double *>(s_.data()); }

private:
  std::array<char, 8> s_;
};

class Column {
public:
  enum Type { METRIC, DIMENSION };
  enum SortType { STRING, INTEGER, FLOAT };

  Column(const util::Config &config, Type type, size_t index);
  Column(const Column &other) = delete;
  virtual ~Column() {}

  virtual const BaseNumType &num_type() const = 0;
  virtual void Accept(class ColumnVisitor &visitor) const = 0;

  const std::string &name() const { return name_; }
  const std::string &input_field() const { return input_field_; }
  size_t index() const { return index_; }
  Type type() const { return type_; }
  virtual SortType sort_type() const = 0;

private:
  Type type_;
  size_t index_;
  std::string name_;
  std::string input_field_;
};

class Dimension : public Column {
public:
  enum DimType { STRING, NUMERIC, TIME, BOOLEAN };

  Dimension(const util::Config &config, size_t index, DimType dim_type)
      : Column(config, Column::Type::DIMENSION, index), dim_type_(dim_type) {}

  Dimension(const Dimension &other) = delete;

  virtual const BaseNumType &num_type() const = 0;

  DimType dim_type() const { return dim_type_; }

private:
  DimType dim_type_;
};

class StrDimension : public Dimension {
public:
  StrDimension(const util::Config &config, size_t index, Dictionaries &dicts);
  StrDimension(const StrDimension &other) = delete;

  void Accept(class ColumnVisitor &visitor) const;

  int length() const { return length_; }
  uint64_t cardinality() const { return cardinality_; }
  DimensionDict *dict() const { return dict_; }
  const BaseNumType &num_type() const { return num_type_; }
  SortType sort_type() const { return SortType::STRING; }

private:
  const UIntType num_type_;
  uint64_t cardinality_;
  int length_;
  DimensionDict *dict_;
};

class NumDimension : public Dimension {
public:
  NumDimension(const util::Config &config, size_t index);
  NumDimension(const NumDimension &other) = delete;

  void Accept(class ColumnVisitor &visitor) const;

  const NumericType &num_type() const { return num_type_; }

  bool fp() const {
    return num_type_.type() == NumericType::DOUBLE ||
           num_type_.type() == NumericType::FLOAT;
  }

  SortType sort_type() const {
    return fp() ? SortType::FLOAT : SortType::INTEGER;
  }

private:
  const NumericType num_type_;
};

class TimeDimension : public Dimension {
public:
  TimeDimension(const util::Config &config, size_t index, bool micro_precision);
  TimeDimension(const TimeDimension &other) = delete;

  const std::string &format() const { return format_; }
  const std::vector<RollupRule> &rollup_rules() const { return rollup_rules_; }
  const Granularity &granularity() const { return granularity_; }
  bool micro_precision() const { return micro_precision_; }
  const BaseNumType &num_type() const { return num_type_; }
  SortType sort_type() const { return SortType::STRING; }

  void Accept(class ColumnVisitor &visitor) const;

private:
  std::string format_;
  Granularity granularity_;
  const UIntType num_type_;
  std::vector<RollupRule> rollup_rules_;
  bool micro_precision_;
};

class BoolDimension : public Dimension {
public:
  BoolDimension(const util::Config &config, size_t index);
  BoolDimension(const BoolDimension &other) = delete;

  void Accept(class ColumnVisitor &visitor) const;

  const BaseNumType &num_type() const { return num_type_; }
  SortType sort_type() const { return SortType::INTEGER; }

private:
  const UIntType num_type_;
};

class Metric : public Column {
public:
  enum AggregationType { MAX, MIN, SUM, AVG, COUNT, BITSET };

  Metric(const util::Config &config, size_t index, AggregationType agg_type)
      : Column(config, Column::Type::METRIC, index), agg_type_(agg_type) {}

  Metric(const Metric &other) = delete;

  AggregationType agg_type() const { return agg_type_; }

private:
  const AggregationType agg_type_;
};

class ValueMetric : public Metric {
public:
  ValueMetric(const util::Config &config, size_t index);
  ValueMetric(const Metric &other) = delete;

  void Accept(class ColumnVisitor &visitor) const;

  const BaseNumType &num_type() const { return num_type_; }

  bool fp() const {
    return num_type_.type() == NumericType::DOUBLE ||
           num_type_.type() == NumericType::FLOAT;
  }

  SortType sort_type() const {
    return fp() ? SortType::FLOAT : SortType::INTEGER;
  }

private:
  const NumericType num_type_;
};

class BitsetMetric : public Metric {
public:
  BitsetMetric(const util::Config &config, size_t index);

  BitsetMetric(const BitsetMetric &other) = delete;

  void Accept(class ColumnVisitor &visitor) const;

  const BaseNumType &num_type() const { return num_type_; }
  SortType sort_type() const { return SortType::INTEGER; }

private:
  const UIntType num_type_;
};

class ColumnVisitor {
public:
  virtual ~ColumnVisitor() {}

  virtual void Visit(const StrDimension *dimension) = 0;
  virtual void Visit(const NumDimension *dimension) = 0;
  virtual void Visit(const TimeDimension *dimension) = 0;
  virtual void Visit(const BoolDimension *dimension) = 0;
  virtual void Visit(const ValueMetric *metric) = 0;
  virtual void Visit(const BitsetMetric *metric) = 0;
};
}
}

#endif // VIYA_DB_COLUMN_H_
