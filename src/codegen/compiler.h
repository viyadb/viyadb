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

#ifndef VIYA_CODEGEN_COMPILER_H_
#define VIYA_CODEGEN_COMPILER_H_

#include <unordered_map>
#include <memory>
#include "util/config.h"
#include "codegen/shared_library.h"

namespace viya {
namespace codegen {

namespace util = viya::util;

class Compiler {
  public:
    Compiler(const util::Config& config);
    std::shared_ptr<SharedLibrary> Compile(const std::string& code);

  private:
    std::vector<std::string> GetFunctionNames(const std::string& lib_file);

  private:
    std::vector<std::string> cmd_;
    std::string path_;
    std::unordered_map<uint64_t,std::shared_ptr<SharedLibrary>> libs_;
};

}}

#endif // VIYA_CODEGEN_COMPILER_H_
