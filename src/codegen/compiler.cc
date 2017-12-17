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

#include <stdexcept>
#include <chrono>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <cityhash/src/city.h>
#include <glog/logging.h>
#include "db/defs.h"
#include "util/process.h"
#include "codegen/compiler.h"

namespace viya {
namespace codegen {

namespace fs = boost::filesystem;
namespace bi = boost::interprocess;
namespace cr = std::chrono;

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

  path_ = config.str("state_dir", "/tmp/viyadb") + "/codegen";
  fs::create_directories(fs::path(path_));
}

std::shared_ptr<SharedLibrary> Compiler::Compile(const std::string& code) {
  std::string code_and_version(code + GIT_SHA1);
  uint64_t code_hash = CityHash64(code_and_version.c_str(), code_and_version.size());

  auto library = libs_[code_hash];
  if (library == nullptr) {
    std::string prefix = path_ + "/" + std::to_string(code_hash);
    std::string so_file = prefix + ".so";
    std::string tmp_so_file = prefix + "_.so";
    std::string lock_file = so_file + ".lock";

    fs::ofstream(lock_file.c_str());
    bi::file_lock fl(lock_file.c_str());
    bi::scoped_lock<bi::file_lock> lock(fl);

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
