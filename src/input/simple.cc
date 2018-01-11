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

#include "input/simple.h"
#include "util/config.h"
#include <vector>

namespace viya {
namespace input {

SimpleLoader::SimpleLoader(db::Table &table) : Loader(util::Config{}, table) {}

SimpleLoader::SimpleLoader(const util::Config &config, db::Table &table)
    : Loader(config, table) {}

void SimpleLoader::Load(std::initializer_list<std::vector<std::string>> rows) {
  BeforeLoad();
  for (auto row : rows) {
    Loader::Load(row);
  }
  AfterLoad();
}
}
}
