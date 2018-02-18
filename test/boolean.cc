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

#include "db/database.h"
#include "db/table.h"
#include "input/simple.h"
#include "query/output.h"
#include "util/config.h"
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace db = viya::db;
namespace util = viya::util;
namespace query = viya::query;
namespace input = viya::input;

using json = nlohmann::json;

class BoolDimEvents : public testing::Test {
protected:
  BoolDimEvents()
      : db(std::move(util::Config(json{
            {"tables",
             {{{"name", "events"},
               {"dimensions",
                {{{"name", "event_name"}},
                 {{"name", "is_organic"}, {"type", "boolean"}}}},
               {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}}))) {}
  db::Database db;
};

TEST_F(BoolDimEvents, QueryTest) {
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"purchase", "true"},
               {"purchase", "false"},
               {"open", "true"},
               {"open", "false"},
               {"open", "true"}});

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{
               {"type", "aggregate"},
               {"table", "events"},
               {"dimensions", {"event_name", "is_organic"}},
               {"metrics", {"count"}},
               {"filter",
                {{"op", "eq"}, {"column", "is_organic"}, {"value", "true"}}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"purchase", "true", "1"}, {"open", "true", "2"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}
