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

#ifndef VIYA_UTIL_LATCH_H_
#define VIYA_UTIL_LATCH_H_

#include <condition_variable>
#include <mutex>

namespace viya {
namespace util {

class CountDownLatch {
public:
  explicit CountDownLatch(size_t count) : count_(count) {}
  CountDownLatch(const CountDownLatch &other) = delete;
  CountDownLatch &operator=(const CountDownLatch &other) = delete;

  void CountDown() {
    std::lock_guard<std::mutex> lock{mtx_};
    --count_;
    if (count_ == 0) {
      cond_.notify_all();
    }
  }

  void Wait() {
    std::unique_lock<std::mutex> lock{mtx_};
    cond_.wait(lock, [this]() { return count_ == 0; });
  }

private:
  std::mutex mtx_;
  std::condition_variable cond_;
  size_t count_;
};

} // namespace util
} // namespace viya

#endif // VIYA_UTIL_LATCH_H_
