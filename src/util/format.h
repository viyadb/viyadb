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

#ifndef VIYA_UTIL_FORMAT_H_
#define VIYA_UTIL_FORMAT_H_

#define FMT_HEADER_ONLY
#include <fmt/string.h>
#include <ctime>
#include <cstdio>

namespace viya {
namespace util {

class Format {
  public:
    Format():w_(buf_, sizeof(buf_)) {}

    template<typename T>
    const char* num(T n) {
      w_.clear();
      w_<<n;
      return w_.c_str();
    }

    const char* num(double n) {
      snprintf(buf_, sizeof(buf_), "%.15g", n);
      return buf_;
    }

    const char* num(int8_t n) {
      w_.clear();
      w_<<(int32_t)n;
      return w_.c_str();
    }

    const char* num(uint8_t n) {
      w_.clear();
      w_<<(uint32_t)n;
      return w_.c_str();
    }

    const char* num(int16_t n) {
      w_.clear();
      w_<<(int32_t)n;
      return w_.c_str();
    }

    const char* num(uint16_t n) {
      w_.clear();
      w_<<(uint32_t)n;
      return w_.c_str();
    }

    const char* date(const char* fmt, const std::tm& tm) {
      strftime(buf_, sizeof(buf_), fmt, &tm);
      return buf_;
    }

    const char* date(const char* fmt, const uint32_t ts) {
      time_t tts = (time_t) ts;
      gmtime_r(&tts, &tm_);
      return date(fmt, tm_);
    }

  private:
    std::tm tm_;
    char buf_[250];
    fmt::ArrayWriter w_;
};

}}

#endif // VIYA_UTIL_FORMAT_H_
