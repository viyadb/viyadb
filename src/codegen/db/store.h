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

#ifndef VIYA_CODEGEN_DB_STORE_H_
#define VIYA_CODEGEN_DB_STORE_H_

#include "codegen/generator.h"
#include "db/rollup.h"
#include "db/store.h"
#include "util/macros.h"
#include <string>
#include <vector>

namespace viya {
namespace db {

class Dimension;
class Metric;
class Table;
class SegmentBase;

} // namespace db
} // namespace viya

namespace viya {
namespace codegen {

namespace db = viya::db;

class Compiler;

// This tuple structure is used:
//  - During ingestion, representing an incoming row
//  - During query to aggregate results
//
class TupleStruct : public CodeGenerator {
public:
  TupleStruct(const std::vector<const db::Dimension *> &dimensions,
              const std::vector<const db::Metric *> &metrics,
              const std::string &struct_name)
      : dimensions_(dimensions), metrics_(metrics), struct_name_(struct_name) {}

  DISALLOW_COPY_AND_MOVE(TupleStruct);

  Code GenerateCode() const;

private:
  const std::vector<const db::Dimension *> &dimensions_;
  const std::vector<const db::Metric *> &metrics_;
  const std::string struct_name_;
};

class SegmentStatsStruct : public CodeGenerator {
public:
  SegmentStatsStruct(const db::Table &table) : table_(table) {}
  DISALLOW_COPY_AND_MOVE(SegmentStatsStruct);

  Code GenerateCode() const;

private:
  const db::Table &table_;
};

class StoreDefs : public CodeGenerator {
public:
  StoreDefs(const db::Table &table) : table_(table) {}

  DISALLOW_COPY_AND_MOVE(StoreDefs);

  Code GenerateCode() const;

private:
  const db::Table &table_;
};

class UpsertContextDefs : public CodeGenerator {
public:
  UpsertContextDefs(const db::Table &table) : table_(table) {}
  DISALLOW_COPY_AND_MOVE(UpsertContextDefs);

  Code GenerateCode() const;

private:
  bool AddOptimize() const;
  bool HasTimeDimension() const;
  Code UpsertContextStruct() const;
  Code ResetFunctionCode() const;
  Code OptimizeFunctionCode() const;

private:
  const db::Table &table_;
};

using UpsertContextFn = void *(*)(const db::Table &table);
using CreateSegmentFn = db::SegmentBase *(*)();

class StoreFunctions : public FunctionGenerator {
public:
  StoreFunctions(Compiler &compiler, const db::Table &table)
      : FunctionGenerator(compiler), table_(table) {}
  DISALLOW_COPY_AND_MOVE(StoreFunctions);

  Code GenerateCode() const;

  CreateSegmentFn CreateSegmentFunction();
  UpsertContextFn UpsertContextFunction();

private:
  const db::Table &table_;
};

} // namespace codegen
} // namespace viya

#endif // VIYA_CODEGEN_DB_STORE_H_
