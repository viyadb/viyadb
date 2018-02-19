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

#include <nlohmann/json_fwd.hpp>
#include <string>
#include <vector>

namespace viya {
namespace util {

using json = nlohmann::json;

class Config {
public:
  Config();
  Config(json *conf);
  Config(const json &conf);
  Config(const std::string &content);
  Config(const Config &other);
  Config(Config &&other);
  Config &operator=(const Config &other);
  Config &operator=(Config &&other);
  ~Config();

  bool exists(const std::string &key) const;
  void erase(const std::string &key);

  Config sub(const std::string &key, bool return_empty = false) const;
  std::vector<Config> sublist(const std::string &key) const;
  void set_sub(const std::string &key, Config &sub);

  std::string str(const std::string &key) const;
  std::string str(const std::string &key, const std::string &default_value) const;
  std::vector<std::string> strlist(const std::string &key) const;
  std::vector<std::string>
  strlist(const std::string &key, std::vector<std::string> default_value) const;
  void set_str(const std::string &key, const std::string &value);
  void set_strlist(const std::string &key, std::vector<std::string> value);

  long num(const std::string &key) const;
  long num(const std::string &key, long default_value) const;
  std::vector<long> numlist(const std::string &key) const;
  std::vector<uint32_t> numlist_uint32(const std::string &key) const;
  void set_num(const std::string &key, long value);
  void set_numlist(const std::string &key, std::vector<long> value);

  bool boolean(const std::string &key) const;
  bool boolean(const std::string &key, bool default_value) const;
  void set_boolean(const std::string &key, bool value);

  std::string dump() const;
  json *json_ptr() const;
  void MergeFrom(const Config &other);

private:
  void ValidateKey(const std::string &key) const;

private:
  json *conf_;
};

} // namespace util
} // namespace viya

#endif /* VIYA_UTIL_CONFIG_H_ */
