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

#ifndef VIYA_INPUT_LOADER_FACTORY_H_
#define VIYA_INPUT_LOADER_FACTORY_H_

#include "input/loader.h"

namespace viya { namespace db { class Database; class Table; }}
namespace viya { namespace util { class Config; }}

namespace viya {
namespace input {

namespace db = viya::db;
namespace util = viya::util;

class LoaderFactory {
  public:
    Loader* Create(const util::Config& config, db::Database& database);

  private:
    std::vector<int> CreateTupleIdxMapping(const util::Config& config,
                                           const db::Table& table);
};

}}

#endif // VIYA_INPUT_LOADER_FACTORY_H_
