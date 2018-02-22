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

#ifndef VIYA_CODEGEN_QUERY_SELECT_QUERY_H_
#define VIYA_CODEGEN_QUERY_SELECT_QUERY_H_

#include "codegen/generator.h"
#include "query/query.h"
#include "query/runner.h"

namespace viya {
namespace codegen {

namespace query = viya::query;

class SelectQueryGenerator : public FunctionGenerator {
public:
  SelectQueryGenerator(Compiler &compiler, query::SelectQuery &query)
      : FunctionGenerator(compiler), query_(query) {}

  SelectQueryGenerator(const SelectQueryGenerator &other) = delete;

  Code GenerateCode() const;
  query::SelectQueryFn Function();

private:
  query::SelectQuery &query_;
};

} // namespace codegen
} // namespace viya

#endif // VIYA_CODEGEN_QUERY_SELECT_QUERY_H_
