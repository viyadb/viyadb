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

#include "db.h"
#include "db/database.h"
#include "db/table.h"
#include "query/output.h"
#include "util/config.h"
#include <algorithm>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <utility>

namespace util = viya::util;
namespace query = viya::query;

using json = nlohmann::json;

TEST_F(UserEvents, BitsetMetric) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(util::Config(json{
               {"type", "aggregate"},
               {"table", "events"},
               {"dimensions", {"country"}},
               {"metrics", {"user_id"}},
               {"filter",
                {{"op", "gt"}, {"column", "time"}, {"value", "1495475514"}}}}),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"RU", "1"}, {"IL", "1"}, {"US", "3"}, {"KZ", "2"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}
