/*
 * Copyright (c) 2017 ViyaDB Group
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

#ifndef VIYA_UTIL_SANITIZE_H_
#define VIYA_UTIL_SANITIZE_H_

#include <string>

namespace viya {
namespace util {

/**
 * Checks whether the string can be safely used when generating a C++ code
 */
void check_legal_string(const std::string& what, const std::string& str);

}}

#endif // VIYA_UTIL_SANITIZE_H_
