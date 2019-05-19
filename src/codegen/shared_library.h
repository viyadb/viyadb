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

#ifndef VIYA_CODEGEN_SHARED_LIBRARY_H_
#define VIYA_CODEGEN_SHARED_LIBRARY_H_

#include "util/macros.h"
#include <string>
#include <vector>

namespace viya {
namespace codegen {

class SharedLibrary {
public:
  SharedLibrary(const std::string &path);
  DISALLOW_COPY_AND_MOVE(SharedLibrary);
  ~SharedLibrary();

  template <typename Func> Func GetFunction(const std::string &name) {
    return reinterpret_cast<Func>(GetFunctionPtr(name));
  }

private:
  std::string path_;
  std::vector<std::string> fn_names_;
  void *handle_;
  void *GetFunctionPtr(const std::string &name);
};

} // namespace codegen
} // namespace viya

#endif // VIYA_CODEGEN_SHARED_LIBRARY_H_
