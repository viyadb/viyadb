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

#ifndef VIYA_SQL_STATEMENT_H_
#define VIYA_SQL_STATEMENT_H_

#include <json.hpp>

namespace viya {
namespace sql {

using json = nlohmann::json;

class Statement {
  public:
    enum Type { QUERY, LOAD };

    Statement(Type type):type_(type) {}

    Type type() const { return type_; }
    json& descriptor() { return descriptor_; }

  private:
    Type type_;
    json descriptor_;
};

}}

#endif // VIYA_SQL_DRIVER_H_
