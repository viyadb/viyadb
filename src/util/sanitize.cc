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

#include "util/sanitize.h"
#include <stdexcept>
#include <algorithm>

namespace viya {
namespace util {

void check_legal_string(const std::string& what, const std::string& str) {
  std::string illegal_chars("\"\\");
  auto w = std::find_if(str.begin(), str.end(), [&illegal_chars] (char ch) {
    return illegal_chars.find(ch) != std::string::npos;
  });
  if(w != str.end()) {
    throw std::invalid_argument(
      what + " contains illegal character at (" + std::to_string(std::distance(str.begin(), w)) + "): " + str);
  }
}

}}
