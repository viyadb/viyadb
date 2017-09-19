#ifndef VIYA_CODEGEN_GENERATOR_H_
#define VIYA_CODEGEN_GENERATOR_H_

#include <stdint.h>
#include <sstream>
#include <vector>
#include "codegen/compiler.h"

namespace viya {
namespace codegen {

class Code {
  public:
    Code() {}
    Code(const Code& other) = delete;
    Code(Code&& other);

    void AddHeaders(std::vector<std::string> headers) {
      headers_.insert(headers_.end(), headers.begin(), headers.end());
    }

    template<typename T>
    Code& operator<<(const T& v) {
      body_<<v;
      return *this;
    }

    Code& operator<<(const Code& c) {
      body_<<c.body_.str();
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

class FunctionGenerator: public CodeGenerator {
  public:
    FunctionGenerator(Compiler& compiler):compiler_(compiler) {}

    virtual ~FunctionGenerator() {}

  protected:
    template <typename Func>
    Func GenerateFunction(const std::string& func_name) {
      if (code_.empty()) {
        code_ = GenerateCode().str();
      }
      auto library = compiler_.Compile(code_);
      return library->GetFunction<Func>(func_name);
    }

  private:
    std::string code_;
    Compiler& compiler_;
};

}};

#endif // VIYA_CODEGEN_GENERATOR_H_
