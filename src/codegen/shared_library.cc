#include <stdexcept>
#include <dlfcn.h>
#include <algorithm>
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
