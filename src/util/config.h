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

#ifndef VIYA_UTIL_CONFIG_H_
#define VIYA_UTIL_CONFIG_H_

#include <string>
#include <vector>

namespace viya {
namespace util {

class Config {
public:
  Config();
  Config(void *conf);
  Config(const std::string &content);
  Config(const Config &other);
  Config(Config &&other);
  Config &operator=(const Config &other);
  Config &operator=(Config &&other);
  ~Config();

  bool exists(const char *key) const;
  void erase(const char *key);

  Config sub(const char *key, bool return_empty = false) const;
  std::vector<Config> sublist(const char *key) const;
  void set_sub(const char *key, Config &sub);

  std::string str(const char *key) const;
  std::string str(const char *key, const char *default_value) const;
  std::vector<std::string> strlist(const char *key) const;
  std::vector<std::string>
  strlist(const char *key, std::vector<std::string> default_value) const;
  void set_str(const char *key, const char *value);
  void set_str(const char *key, const std::string &value);
  void set_strlist(const char *key, std::vector<std::string> value);

  long num(const char *key) const;
  long num(const char *key, long default_value) const;
  std::vector<long> numlist(const char *key) const;
  std::vector<uint32_t> numlist_uint32(const char *key) const;
  void set_num(const char *key, long value);
  void set_numlist(const char *key, std::vector<long> value);

  bool boolean(const char *key) const;
  bool boolean(const char *key, bool default_value) const;
  void set_boolean(const char *key, bool value);

  std::string dump() const;
  void *json_ptr() const;
  void MergeFrom(const Config &other);

private:
  void ValidateKey(const char *key) const;

private:
  void *conf_;
};
}
}

#endif /* VIYA_UTIL_CONFIG_H_ */
