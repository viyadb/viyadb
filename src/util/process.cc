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

#include "util/process.h"
#include <cpp-subprocess/subprocess.hpp>

namespace viya {
namespace util {

namespace sp = subprocess;

int Process::Run(const std::vector<std::string> &cmd) {
  return sp::Popen(cmd).wait();
}

int Process::RunWithInput(const std::vector<std::string> &cmd,
                          const std::string &input) {
  auto p = sp::Popen(cmd, sp::input{sp::PIPE});
  p.communicate(input.c_str(), input.size());
  return p.wait();
}
}
}
