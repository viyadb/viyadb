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

#include "input/loader_factory.h"
#include "db/database.h"
#include "input/file_loader.h"
#include "util/config.h"
#include <stdexcept>

namespace viya {
namespace input {

Loader *LoaderFactory::Create(const util::Config &config,
                              db::Database &database) {
  auto table = database.GetTable(config.str("table"));

  std::string type = config.str("type");
  if (type == "file") {
    return new FileLoader(config, *table);
  }
  throw std::invalid_argument("Unsupported input type: " + type);
}
} // namespace input
} // namespace viya
