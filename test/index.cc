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
#include "input/simple.h"
#include "query/output.h"
#include "util/config.h"
#include <algorithm>
#include <gtest/gtest.h>

namespace db = viya::db;
namespace util = viya::util;
namespace query = viya::query;
namespace input = viya::input;

class LiteEvents : public testing::Test {
protected:
  LiteEvents()
      : db(std::move(util::Config(json{
            {"tables",
             {{{"name", "events"},
               {"dimensions",
                {{{"name", "time"}, {"type", "ulong"}, {"max", "4000000"}},
                 {{"name", "dummy"}, {"max", 1}}}},
               {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}}))) {}
  db::Database db;
};

TEST_F(LiteEvents, SegmentsSkipped) {
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  std::vector<std::string> row(2);
  row[1] = "";
  unsigned int start_time = 1996111U;
  unsigned int end_time = start_time + table->segment_size() +
                          std::min(table->segment_size(), 100UL);
  for (unsigned int time = start_time; time < end_time; ++time) {
    row[0] = std::to_string(time);
    loader.Load(row);
  }

  unsigned int segment2_start = start_time + table->segment_size();
  query::MemoryRowOutput output;
  auto stats = db.Query(
      std::move(util::Config(
          json{{"type", "aggregate"},
               {"table", "events"},
               {"dimensions", {"time"}},
               {"metrics", {"count"}},
               {"filter",
                {{"op", "and"},
                 {"filters",
                  {{{"op", "gt"},
                    {"column", "time"},
                    {"value", std::to_string(segment2_start)}},
                   {{"op", "ne"}, {"column", "dummy"}, {"value", "bla"}}}}}}})),
      output);

  EXPECT_EQ(1, stats.scanned_segments);
}
