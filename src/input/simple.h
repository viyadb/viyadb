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

#ifndef VIYA_INPUT_SIMPLE_H_
#define VIYA_INPUT_SIMPLE_H_

#include "input/loader.h"
#include "input/loader_desc.h"

namespace viya {
namespace db {
class Table;
}
} // namespace viya
namespace viya {
namespace util {
class Config;
}
} // namespace viya

namespace viya {
namespace input {

namespace input = viya::input;
namespace db = viya::db;
namespace util = viya::util;

class SimpleLoader : public Loader {
public:
  SimpleLoader(db::Table &table);
  SimpleLoader(const util::Config &config, db::Table &table);
  SimpleLoader(const SimpleLoader &other) = delete;

  void Load(std::initializer_list<std::vector<std::string>> rows);
  void Load(std::vector<std::string> &values) { Loader::Load(values); }
  void LoadData() {}
};
} // namespace input
} // namespace viya

#endif // VIYA_INPUT_SIMPLE_H_
