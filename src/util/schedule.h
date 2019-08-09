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

#ifndef VIYA_UTIL_SCHEDULE_H_
#define VIYA_UTIL_SCHEDULE_H_

#include <atomic>
#include <chrono>
#include <future>

namespace viya {
namespace util {

class Later {
public:
  template <class Func> Later(uint64_t after_ms, Func &&callback) {
    std::thread([after_ms, callback]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(after_ms));
      callback();
    })
        .detach();
  }
};

class Repeat {
public:
  template <class Func>
  Repeat(uint64_t every_ms, Func &&callback) : running_(true) {
    thread_ = std::thread([every_ms, callback, this]() {
      do {
        std::this_thread::sleep_for(std::chrono::milliseconds(every_ms));
        if (running_) {
          callback();
        }
      } while (running_);
    });
  }

  ~Repeat() {
    running_ = false;
    thread_.join();
  }

private:
  std::atomic<bool> running_;
  std::thread thread_;
};

class Always {
public:
  template <class Func> Always(Func &&callback) : running_(true) {
    thread_ = std::thread([callback, this]() {
      while (running_) {
        callback();
      }
    });
  }

  ~Always() {
    running_ = false;
    thread_.join();
  }

private:
  std::atomic<bool> running_;
  std::thread thread_;
};

class WaitFor {
public:
  template <class Func>
  WaitFor(uint64_t timeout_ms, Func &&callback) : running_(true) {
    thread_ = std::thread([timeout_ms, callback, this]() {
      uint8_t retries = 0;
      uint64_t remaining_time = timeout_ms;
      do {
        auto wait_ms = std::min(2 ^ retries * 200UL, remaining_time);
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));
        remaining_time -= wait_ms;
        running_ = !callback() && remaining_time > 0;
        ++retries;
      } while (running_);

      if (remaining_time <= 0) {
        throw std::runtime_error("Timeout waiting for operation");
      }
    });
  }

  ~WaitFor() {
    running_ = false;
    thread_.join();
  }

private:
  std::atomic<bool> running_;
  std::thread thread_;
};

} // namespace util
} // namespace viya

#endif // VIYA_UTIL_SCHEDULE_H_
