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

#include "db/dictionary.h"

namespace viya {
namespace db {

DimensionDict::DimensionDict(const BaseNumType &code_type)
    : size_(code_type.size()) {
  std::string exceeded_value("__exceeded");
  c2v_.push_back(exceeded_value);

  switch (size_) {
  case BaseNumType::Size::_1:
    v2c_ = new DictImpl<uint8_t>(exceeded_value);
    break;
  case BaseNumType::Size::_2:
    v2c_ = new DictImpl<uint16_t>(exceeded_value);
    break;
  case BaseNumType::Size::_4:
    v2c_ = new DictImpl<uint32_t>(exceeded_value);
    break;
  case BaseNumType::Size::_8:
    v2c_ = new DictImpl<uint64_t>(exceeded_value);
    break;
  default:
    throw std::runtime_error("Unsupported dimension code size!");
  }
}

// Using this method should be reduced to non-intensive parts only:
AnyNum DimensionDict::Decode(const std::string &value) {
  folly::RWSpinLock::ReadHolder guard(lock_);
  AnyNum code;
  switch (size_) {
  case BaseNumType::Size::_1: {
    auto typed_dict = reinterpret_cast<DictImpl<uint8_t> *>(v2c_);
    auto it = typed_dict->find(value);
    code = AnyNum((uint8_t)(it != typed_dict->end() ? it->second : UINT8_MAX));
  } break;
  case BaseNumType::Size::_2: {
    auto typed_dict = reinterpret_cast<DictImpl<uint16_t> *>(v2c_);
    auto it = typed_dict->find(value);
    code =
        AnyNum((uint16_t)(it != typed_dict->end() ? it->second : UINT16_MAX));
  } break;
  case BaseNumType::Size::_4: {
    auto typed_dict = reinterpret_cast<DictImpl<uint32_t> *>(v2c_);
    auto it = typed_dict->find(value);
    code =
        AnyNum((uint32_t)(it != typed_dict->end() ? it->second : UINT32_MAX));
  } break;
  case BaseNumType::Size::_8: {
    auto typed_dict = reinterpret_cast<DictImpl<uint64_t> *>(v2c_);
    auto it = typed_dict->find(value);
    code =
        AnyNum((uint64_t)(it != typed_dict->end() ? it->second : UINT64_MAX));
  } break;
  }
  return code;
}

DimensionDict::~DimensionDict() {
  switch (size_) {
  case BaseNumType::Size::_1:
    delete reinterpret_cast<DictImpl<uint8_t> *>(v2c_);
    break;
  case BaseNumType::Size::_2:
    delete reinterpret_cast<DictImpl<uint16_t> *>(v2c_);
    break;
  case BaseNumType::Size::_4:
    delete reinterpret_cast<DictImpl<uint32_t> *>(v2c_);
    break;
  case BaseNumType::Size::_8:
    delete reinterpret_cast<DictImpl<uint64_t> *>(v2c_);
    break;
  }
}

Dictionaries::~Dictionaries() {
  for (auto &it : dicts_) {
    delete it.second;
  }
}

DimensionDict *Dictionaries::GetOrCreate(const std::string &dim_name,
                                         const BaseNumType &code_type) {
  folly::RWSpinLock::WriteHolder guard(lock_);
  DimensionDict *dict = nullptr;
  auto it = dicts_.find(dim_name);
  if (it == dicts_.end()) {
    dict = new DimensionDict(code_type);
    dicts_.insert(std::make_pair(dim_name, dict));
  } else {
    dict = it->second;
  }
  return dict;
}
}
}
