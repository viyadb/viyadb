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

#ifndef VIYA_CODEGEN_DB_METADATA_H_
#define VIYA_CODEGEN_DB_METADATA_H_

#include "codegen/generator.h"
#include "db/table.h"

namespace viya {
namespace codegen {

namespace db = viya::db;

using TableMetadataFn = void (*)(db::Table &, std::string &);

class TableMetadata : public FunctionGenerator {
public:
  TableMetadata(Compiler &compiler, const db::Table &table)
      : FunctionGenerator(compiler), table_(table) {}

  TableMetadata(const TableMetadata &other) = delete;

  Code GenerateCode() const;
  TableMetadataFn Function();

private:
  const db::Table &table_;
};
} // namespace codegen
} // namespace viya

#endif // VIYA_CODEGEN_DB_METADATA_H_
