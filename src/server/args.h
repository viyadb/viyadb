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

#ifndef VIYA_SERVER_ARGS_H_
#define VIYA_SERVER_ARGS_H_

#include "util/config.h"
#include <vector>

namespace viya {
namespace server {

namespace util = viya::util;

class CmdlineArgs {
public:
  CmdlineArgs() {}
  util::Config Parse(std::vector<std::string> args);

private:
  std::string Help();
  util::Config Defaults();
  util::Config OpenConfig(const std::string &file);
};
}
}

#endif // VIYA_SERVER_ARGS_H_
