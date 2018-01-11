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
#include <string>
#include <vector>

namespace viya {
namespace db {

class Dimension;
class Metric;
class Table;
}
}

namespace viya {
namespace codegen {

namespace db = viya::db;

class Compiler;

class DimensionsStruct : public CodeGenerator {
public:
  DimensionsStruct(const std::vector<const db::Dimension *> &dimensions,
                   std::string struct_name)
      : dimensions_(dimensions), struct_name_(struct_name) {}

  DimensionsStruct(const DimensionsStruct &other) = delete;

  Code GenerateCode() const;

private:
  const std::vector<const db::Dimension *> &dimensions_;
  std::string struct_name_;
};

class MetricsStruct : public CodeGenerator {
public:
  MetricsStruct(const std::vector<const db::Metric *> &metrics,
                std::string struct_name)
      : metrics_(metrics), struct_name_(struct_name) {}

  MetricsStruct(const MetricsStruct &other) = delete;

  Code GenerateCode() const;

private:
  const std::vector<const db::Metric *> &metrics_;
  std::string struct_name_;
};

class SegmentStatsStruct : public CodeGenerator {
public:
  SegmentStatsStruct(const db::Table &table) : table_(table) {}
  SegmentStatsStruct(const SegmentStatsStruct &other) = delete;

  Code GenerateCode() const;

private:
  const db::Table &table_;
};

class StoreDefs : public CodeGenerator {
public:
  StoreDefs(const db::Table &table) : table_(table) {}

  StoreDefs(const StoreDefs &other) = delete;

  Code GenerateCode() const;

private:
  const db::Table &table_;
};

class CreateSegment : public FunctionGenerator {
public:
  CreateSegment(Compiler &compiler, const db::Table &table)
      : FunctionGenerator(compiler), table_(table) {}

  CreateSegment(const CreateSegment &other) = delete;

  Code GenerateCode() const;
  db::CreateSegmentFn Function();

private:
  const db::Table &table_;
};
}
}

#endif // VIYA_CODEGEN_DB_STORE_H_
