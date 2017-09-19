#ifndef VIYA_CODEGEN_SHARED_LIBRARY_H_
#define VIYA_CODEGEN_SHARED_LIBRARY_H_

#include <string>

namespace viya {
namespace codegen {

class SharedLibrary {
  public:
    SharedLibrary(const std::string& path);
    SharedLibrary(const SharedLibrary& other) = delete;
    ~SharedLibrary();

    template <typename Func>
    Func GetFunction(const std::string& name) {
      return reinterpret_cast<Func>(GetFunctionPtr(name));
    }

  private:
    std::string path_;
    std::vector<std::string> fn_names_;
    void* handle_;
    void* GetFunctionPtr(const std::string& name);
};

}}

#endif // VIYA_CODEGEN_SHARED_LIBRARY_H_
