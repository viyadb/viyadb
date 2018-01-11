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

#ifndef VIYA_DB_ROLLUP_H_
#define VIYA_DB_ROLLUP_H_

#include "util/config.h"
#include "util/time.h"
#include <string>

namespace viya {
namespace db {

namespace util = viya::util;

class Granularity {
public:
  Granularity() : time_unit_(util::TimeUnit::_UNDEFINED) {}
  Granularity(const std::string &name);

  bool empty() const { return time_unit_ == util::TimeUnit::_UNDEFINED; }
  util::TimeUnit time_unit() const { return time_unit_; }

private:
  util::TimeUnit time_unit_;
};

class RollupRule {
public:
  RollupRule(const util::Config &conf);

  const util::Duration &after() const { return after_; }
  const Granularity &granularity() const { return granularity_; }

private:
  Granularity granularity_;
  util::Duration after_;
};
}
}

#endif // VIYA_DB_ROLLUP_H_
