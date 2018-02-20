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

#ifndef VIYA_CODEGEN_DB_ROLLUP_H_
#define VIYA_CODEGEN_DB_ROLLUP_H_

#include "codegen/generator.h"
#include "db/rollup.h"

namespace viya {
namespace db {

class Dimension;
class TimeDimension;

} // namespace db
} // namespace viya

namespace viya {
namespace codegen {

namespace db = viya::db;

class RollupDefs : public CodeGenerator {
public:
  RollupDefs(const std::vector<const db::Dimension *> &dimensions)
      : dimensions_(dimensions) {}

  RollupDefs(const RollupDefs &other) = delete;

  Code GenerateCode() const;

private:
  const std::vector<const db::Dimension *> &dimensions_;
};

class RollupReset : public CodeGenerator {
public:
  RollupReset(const std::vector<const db::Dimension *> &dimensions)
      : dimensions_(dimensions) {}

  RollupReset(const RollupReset &other) = delete;

  Code GenerateCode() const;

private:
  const std::vector<const db::Dimension *> &dimensions_;
};

class TimestampRollup : public CodeGenerator {
public:
  TimestampRollup(const db::TimeDimension *dimension,
                  const std::string &var_name,
                  const std::string &prefix = std::string())
      : dimension_(dimension), var_name_(var_name), prefix_(prefix) {}

  TimestampRollup(const TimestampRollup &other) = delete;

  Code GenerateCode() const;

private:
  const db::TimeDimension *dimension_;
  std::string var_name_;
  std::string prefix_;
};

} // namespace codegen
} // namespace viya

#endif // VIYA_CODEGEN_DB_ROLLUP_H_
