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

#ifndef VIYA_CODEGEN_QUERY_FILTER_H_
#define VIYA_CODEGEN_QUERY_FILTER_H_

#include "codegen/generator.h"
#include "db/column.h"
#include "query/filter.h"

namespace viya {
namespace db {

class Table;

} // namespace db
} // namespace viya

namespace viya {
namespace codegen {

namespace query = viya::query;

class FilterArgsPacker : public query::FilterVisitor {
public:
  FilterArgsPacker(const db::Table &table) : table_(table) {}

  void Visit(const query::RelOpFilter *filter);
  void Visit(const query::InFilter *filter);
  void Visit(const query::CompositeFilter *filter);
  void Visit(const query::EmptyFilter *filter);

  const std::vector<db::AnyNum> &args() const { return args_; }

private:
  const db::Table &table_;
  std::vector<db::AnyNum> args_;
};

class FilterArgsUnpack : public CodeGenerator {
public:
  FilterArgsUnpack(const db::Table &table, const query::Filter *filter,
                   const std::string &var_prefix)
      : table_(table), filter_(filter), var_prefix_(var_prefix) {}
  FilterArgsUnpack(const FilterArgsUnpack &other) = delete;

  Code GenerateCode() const;

private:
  const db::Table &table_;
  const query::Filter *filter_;
  const std::string var_prefix_;
};

class SegmentSkip : CodeGenerator {
public:
  SegmentSkip(const db::Table &table, const query::Filter *filter)
      : table_(table), filter_(filter) {}

  SegmentSkip(const SegmentSkip &other) = delete;

  Code GenerateCode() const;

private:
  const db::Table &table_;
  const query::Filter *filter_;
};

class FilterComparison : CodeGenerator {
public:
  FilterComparison(const db::Table &table, const query::Filter *filter,
                   const std::string &var_prefix)
      : table_(table), filter_(filter), var_prefix_(var_prefix) {}

  FilterComparison(const FilterComparison &other) = delete;

  Code GenerateCode() const;

private:
  const db::Table &table_;
  const query::Filter *filter_;
  const std::string var_prefix_;
};

} // namespace codegen
} // namespace viya

#endif // VIYA_CODEGEN_QUERY_FILTER_H_
