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

#ifndef VIYA_UTIL_STRING_H_
#define VIYA_UTIL_STRING_H_

#include <string>

namespace viya {
namespace util {

/**
 * This utility implements comparison methods for strings containing numbers
 */
class StringNumCmp {
public:
  static bool GreaterInt(const std::string &str1, const std::string &str2) {
    size_t size1 = str1.size();
    size_t size2 = str2.size();
    return size1 != size2 ? (size1 > size2 ? true : false) : (str1 > str2);
  }

  static bool SmallerInt(const std::string &str1, const std::string &str2) {
    size_t size1 = str1.size();
    size_t size2 = str2.size();
    return size1 != size2 ? (size1 < size2 ? true : false) : (str1 < str2);
  }

  static bool GreaterFloat(const std::string &str1, const std::string &str2) {
    return std::stod(str1) > std::stod(str2);
  }

  static bool SmallerFloat(const std::string &str1, const std::string &str2) {
    return std::stod(str1) < std::stod(str2);
  }
};

} // namespace util
} // namespace viya

#endif // VIYA_UTIL_STRING_H_
