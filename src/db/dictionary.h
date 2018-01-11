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

#ifndef VIYA_DB_DICTIONARY_H_
#define VIYA_DB_DICTIONARY_H_

#include "db/column.h"
#include "util/rwlock.h"
#include <unordered_map>
#include <vector>

namespace viya {
namespace db {

template <typename V>
class DictImpl : public std::unordered_map<std::string, V> {
public:
  DictImpl(const std::string &exceeded_value) {
    this->insert(std::make_pair(exceeded_value, 0));
  }
};

class DimensionDict {
public:
  DimensionDict(const BaseNumType &code_type);
  ~DimensionDict();

  void *v2c() const { return v2c_; }
  folly::RWSpinLock &lock() { return lock_; }
  std::vector<std::string> &c2v() { return c2v_; }

  AnyNum Decode(const std::string &value);

private:
  BaseNumType::Size size_;
  folly::RWSpinLock lock_;
  std::vector<std::string> c2v_; // code to value
  void *v2c_;                    // value to code
};

class Dictionaries {
public:
  Dictionaries() {}
  ~Dictionaries();

  DimensionDict *GetOrCreate(const std::string &dim_name,
                             const BaseNumType &code_type);

private:
  std::unordered_map<std::string, DimensionDict *> dicts_;
  folly::RWSpinLock lock_;
};
}
}

#endif // VIYA_DB_DICTIONARY_H_
