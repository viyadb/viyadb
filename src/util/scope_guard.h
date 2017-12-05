#ifndef VIYA_UTIL_SCOPE_GUARD_H_
#define VIYA_UTIL_SCOPE_GUARD_H_

#include <functional>

namespace viya {
namespace util {

class ScopeGuard {
  public: 
    template<class Callable> 
    ScopeGuard(Callable&& undo_func):f(std::forward<Callable>(undo_func)) {}

    ScopeGuard(ScopeGuard&& other):f(std::move(other.f)) {
      other.f = nullptr;
    }

    ~ScopeGuard() {
      if(f) f(); // must not throw
    }

    void dismiss() noexcept {
      f = nullptr;
    }

    ScopeGuard(const ScopeGuard&) = delete;
    void operator=(const ScopeGuard&) = delete;

  private:
    std::function<void()> f;
};

}}

#endif // VIYA_UTIL_SCOPE_GUARD_H_
