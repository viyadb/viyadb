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

#ifndef VIYA_CODEGEN_GENERATOR_H_
#define VIYA_CODEGEN_GENERATOR_H_

#include "codegen/compiler.h"
#include <sstream>
#include <vector>

namespace viya {
namespace codegen {

class Code {
public:
  Code() {}
  Code(const Code &other) = delete;
  Code(Code &&other);

  void AddHeaders(std::vector<std::string> headers) {
    headers_.insert(headers_.end(), headers.begin(), headers.end());
  }

  template <typename T> Code &operator<<(const T &v) {
    body_ << v;
    return *this;
  }

  Code &operator<<(const Code &c) {
    body_ << c.body_.str();
    AddHeaders(c.headers_);
    return *this;
  }

  std::string str() const;

private:
  std::ostringstream body_;
  std::vector<std::string> headers_;
};

class CodeGenerator {
public:
  CodeGenerator() {}
  virtual ~CodeGenerator() {}

  virtual Code GenerateCode() const = 0;
};

class FunctionGenerator : public CodeGenerator {
public:
  FunctionGenerator(Compiler &compiler) : compiler_(compiler) {}

  virtual ~FunctionGenerator() {}

protected:
  template <typename Func> Func GenerateFunction(const std::string &func_name) {
    if (code_.empty()) {
      code_ = GenerateCode().str();
    }
    auto library = compiler_.Compile(code_);
    return library->GetFunction<Func>(func_name);
  }

protected:
  Compiler &compiler_;

private:
  std::string code_;
};

} // namespace codegen
} // namespace viya

#endif // VIYA_CODEGEN_GENERATOR_H_
