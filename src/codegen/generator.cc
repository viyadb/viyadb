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

#include "codegen/generator.h"
#include <sstream>
#include <unordered_set>

namespace viya {
namespace codegen {

Code::Code(Code &&other) {
  headers_ = std::move(other.headers_);
  body_ = std::move(other.body_);
}

std::string Code::str() const {
  std::stringstream ss;

  std::unordered_set<std::string> headers_set(headers_.begin(), headers_.end());
  for (auto &header : headers_set) {
    ss << "#include <" << header << ">\n";
  }

  std::unordered_set<std::string> namespaces_set(namespaces_.begin(),
                                                 namespaces_.end());
  for (auto &ns : namespaces_set) {
    ss << "namespace " << ns << ";\n";
  }

  ss << body_.str();
  return ss.str();
}

} // namespace codegen
} // namespace viya
