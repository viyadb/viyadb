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

#include <stdexcept>
#include <dlfcn.h>
#include <glog/logging.h>
#include "codegen/shared_library.h"

namespace viya {
namespace codegen {

SharedLibrary::SharedLibrary(const std::string& path):path_(path) {
  DLOG(INFO)<<"Opening shared library: "<<path;
  handle_ = dlopen(path.c_str(), RTLD_LAZY);
  if (handle_ == nullptr) {
    throw std::runtime_error(
        std::string("Error opening shared library: ") + dlerror());
  }
}

SharedLibrary::~SharedLibrary() {
  DLOG(INFO)<<"Closing shared library: "<<path_;
  if (handle_ != nullptr && dlclose(handle_) != 0) {
    std::terminate();
  }
}

void* SharedLibrary::GetFunctionPtr(const std::string& name) {
  // Clear error state:
  dlerror();

  //DLOG(INFO)<<"Looking for symbol '"<<name<<"' in library: "<<path_;
  void* res = dlsym(handle_, name.c_str());
  char* error = dlerror();
  if (error != nullptr) {
    throw std::runtime_error(error);
  }
  return res;
}

}}
