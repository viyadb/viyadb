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
