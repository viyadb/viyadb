#include <stdexcept>
#include <chrono>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string/join.hpp>
#include <cityhash/src/city.h>
#include <glog/logging.h>
#include "db/defs.h"
#include "util/process.h"
#include "codegen/compiler.h"

namespace viya {
namespace codegen {

namespace fs = boost::filesystem;
namespace cr = std::chrono;

Compiler::Compiler():Compiler(util::Config()) {}

Compiler::Compiler(const util::Config& config) {
  cmd_ = {
    "g++",
    "-std=c++14",

    // start dependencies ==>
#if VIYA_IS_RELEASE
    "-I./include",
    "-I./include/viyadb",
    "-L./lib",
#else
    "-I../src",
    "-L./src/util",
    "-I../third_party",
    "-I../third_party/fmt",
    "-I../third_party/json/src",
    "-I../third_party/CRoaring/cpp",
    "-I../third_party/CRoaring/include",
    "-L./third_party/CRoaring/src",
#endif // VIYA_IS_RELEASE

#if !(CXX_COMPILER_IS_CLANG)
    "-Wl,--whole-archive",
#endif
    "-lroaring",
    "-lviya_util",
#if !(CXX_COMPILER_IS_CLANG)
    "-Wl,--no-whole-archive",
#endif
    // <== end of dependencies

    // start optimizations ==>
#ifdef NDEBUG
    "-O2",
    "-funroll-loops",
    "-march=native",
    "-fvisibility=hidden", 
    "-fno-implement-inlines",
    "-DNDEBUG",
#else
    "-Wall",
    "-Wextra",
    "-g",
#endif // NDEBUG
    // <== end of optimizations

    "-shared",
    "-fPIC",
    "-x", "c++", // Must specify the language to allow reading source from STDIN
    "-",         // Read from STDIN
    "-o",
    ""           // This will hold the target .so file path
  };
  path_ = config.str("tmp_path", "/tmp");
}

std::shared_ptr<SharedLibrary> Compiler::Compile(const std::string& code) {
  std::string code_and_version(code + GIT_SHA1);
  uint64_t code_hash = CityHash64(code_and_version.c_str(), code_and_version.size());

  std::lock_guard<std::mutex> lock(mutex_);

  auto library = libs_[code_hash];
  if (library == nullptr) {

    std::string prefix = path_ + "/" + std::to_string(code_hash);
    std::string so_file = prefix + ".so";
    std::string tmp_so_file = prefix + "_.so";

#ifndef NDEBUG
    std::string cpp_file = prefix + ".cc";
    std::ofstream out(cpp_file);
    out<<code.c_str();
    out.close();
    cmd_[cmd_.size() - 3] = cpp_file;
#endif

    if (!fs::exists(so_file)) {
      cmd_.back() = tmp_so_file;

      LOG(INFO)<<boost::algorithm::join(cmd_, " ");
      DLOG(INFO)<<code;
      auto begin = cr::steady_clock::now();

      if (util::Process::RunWithInput(cmd_, code) != 0) {
        throw std::runtime_error("Can't compile: " + code);
      }

      auto end = cr::steady_clock::now();
      LOG(INFO)<<"Compilation took "<<cr::duration_cast<cr::milliseconds>(end - begin).count()
        <<" ms"<<std::endl;

      fs::rename(fs::path(tmp_so_file), fs::path(so_file));
    }

    library = std::make_shared<SharedLibrary>(so_file);
    libs_[code_hash] = library;
  }

  return library;
}

}}
