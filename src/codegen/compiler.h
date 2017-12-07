#ifndef VIYA_CODEGEN_COMPILER_H_
#define VIYA_CODEGEN_COMPILER_H_

#include <unordered_map>
#include <memory>
#include <mutex>
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
    std::mutex mutex_;
    std::unordered_map<uint64_t,std::shared_ptr<SharedLibrary>> libs_;
};

}}

#endif // VIYA_CODEGEN_COMPILER_H_
