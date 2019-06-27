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

namespace viya {
namespace codegen {

std::string Code::str() const {
  std::stringstream ss;
  for (auto &header : headers_) {
    ss << "#include <" << header << ">\n";
  }
  ss << "\n";
  for (auto &ns : namespaces_) {
    ss << "namespace " << ns << ";\n";
  }
  ss << "\n";
  ss << body_.str();
  return ss.str();
}

} // namespace codegen
} // namespace viya
