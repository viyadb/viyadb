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

#include <stdexcept>
#include <json.hpp>
#include "util/config.h"

namespace viya {
namespace util {

using json = nlohmann::json;

Config::Config():conf_(new json()) {}

Config::Config(void* conf):conf_(conf) {}

Config::Config(const std::string& content) {
  json j = json::parse(content);
  conf_ = new json(j);
}

Config::Config(const Config& other):conf_(new json()) {
  *reinterpret_cast<json*>(conf_) = *reinterpret_cast<json*>(other.conf_);
}  

Config::Config(Config&& other) {
  conf_ = other.conf_;  
  other.conf_ = nullptr;
}  

Config& Config::operator=(const Config& other) {
  *reinterpret_cast<json*>(conf_) = *reinterpret_cast<json*>(other.conf_);
  return *this;
}
Config& Config::operator=(Config&& other) {
  conf_ = other.conf_;  
  other.conf_ = nullptr;
  return *this;
}

Config::~Config() {
  delete reinterpret_cast<json*>(conf_);
}

bool Config::exists(const char* key) const {
  json* j = reinterpret_cast<json*>(conf_);
  return !(j->find(key) == j->end());
}

void Config::ValidateKey(const char* key) const {
  if (!exists(key)) {
    throw std::invalid_argument("Missing configuration key: " + std::string(key));
  }
}

bool Config::boolean(const char* key) const {
  ValidateKey(key);
  try {
    return (*reinterpret_cast<json*>(conf_))[key].get<bool>();
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

bool Config::boolean(const char* key, bool default_value) const {
  if (!exists(key)) {
    return default_value;
  }
  try {
    return (*reinterpret_cast<json*>(conf_))[key].get<bool>();
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

void Config::set_boolean(const char* key, bool value) {
  (*reinterpret_cast<json*>(conf_))[key] = value;
}

long Config::num(const char* key) const {
  ValidateKey(key);
  try {
    return (*reinterpret_cast<json*>(conf_))[key].get<long>();
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

long Config::num(const char* key, long default_value) const {
  if (!exists(key)) {
    return default_value;
  }
  try {
    return (*reinterpret_cast<json*>(conf_))[key].get<long>();
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

std::vector<long> Config::numlist(const char* key) const {
  ValidateKey(key);
  try {
    return (*reinterpret_cast<json*>(conf_))[key].get<std::vector<long>>();
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

std::vector<uint32_t> Config::numlist_uint32(const char* key) const {
  ValidateKey(key);
  try {
    return (*reinterpret_cast<json*>(conf_))[key].get<std::vector<uint32_t>>();
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

void Config::set_num(const char* key, long value) {
  (*reinterpret_cast<json*>(conf_))[key] = value;
}

void Config::set_numlist(const char* key, std::vector<long> value) {
  (*reinterpret_cast<json*>(conf_))[key] = value;
}

std::string Config::str(const char* key) const {
  ValidateKey(key);
  try {
    return (*reinterpret_cast<json*>(conf_))[key].get<std::string>();
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

std::string Config::str(const char* key, const char* default_value) const {
  if (!exists(key)) {
    return default_value;
  }
  try {
    return (*reinterpret_cast<json*>(conf_))[key].get<std::string>();
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

std::vector<std::string> Config::strlist(const char* key) const {
  ValidateKey(key);
  try {
    return (*reinterpret_cast<json*>(conf_))[key].get<std::vector<std::string>>();
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

std::vector<std::string> Config::strlist(const char* key, std::vector<std::string> default_value) const {
  if (!exists(key)) {
    return default_value;
  }
  try {
    return (*reinterpret_cast<json*>(conf_))[key].get<std::vector<std::string>>();
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

void Config::set_str(const char* key, const char* value) {
  (*reinterpret_cast<json*>(conf_))[key] = value;
}

void Config::set_strlist(const char* key, std::vector<std::string> value) {
  (*reinterpret_cast<json*>(conf_))[key] = value;
}

Config Config::sub(const char* key, bool return_empty) const {
  if (!return_empty) {
    ValidateKey(key);
  }
  try {
    return Config(new json((*reinterpret_cast<json*>(conf_))[key].get<json>()));
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

void Config::set_sub(const char* key, Config& sub) {
  (*reinterpret_cast<json*>(conf_))[key] = *reinterpret_cast<json*>(sub.conf_);
}

std::vector<Config> Config::sublist(const char* key) const {
  ValidateKey(key);
  try {
    std::vector<Config> result;
    json* j = reinterpret_cast<json*>(conf_);
    if (j->find(key) != j->end()) {
      for (auto conf : (*j)[key]) {
        result.push_back(Config(new json(conf)));
      }
    }
    return result;
  } catch (std::exception& e) {
    throw std::invalid_argument(std::string(key) + ": " + e.what());
  }
}

std::string Config::dump() const {
  return reinterpret_cast<json*>(conf_)->dump(2);
}

void* Config::json_ptr() const {
  return conf_;
}

void Config::MergeFrom(const Config& other) {
  auto this_json = reinterpret_cast<nlohmann::json*>(conf_);
  auto other_json = reinterpret_cast<nlohmann::json*>(other.conf_);
  for (auto it = other_json->begin(); it != other_json->end(); ++it) {
    (*this_json)[it.key()] = it.value();
  }
}

}}
