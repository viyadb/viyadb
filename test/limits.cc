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
#include "db/table.h"
#include "input/simple.h"
#include "query/output.h"
#include "util/config.h"
#include <algorithm>
#include <gtest/gtest.h>

namespace util = viya::util;
namespace query = viya::query;
namespace input = viya::input;

class LowCardColumn : public testing::Test {
protected:
  LowCardColumn()
      : db(std::move(util::Config(json{
            {"tables",
             {{{"name", "events"},
               {"dimensions", {{{"name", "http_method"}, {"cardinality", 5}}}},
               {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}}))) {}
  db::Database db;
};

class CardinalityGuard : public testing::Test {
protected:
  CardinalityGuard()
      : db(std::move(util::Config(json{
            {"tables",
             {{{"name", "events"},
               {"dimensions",
                {{{"name", "device_id"}},
                 {{"name", "event_name"},
                  {"cardinality_guard",
                   {{"dimensions", {"device_id"}}, {"limit", 3}}}}}},
               {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}}))) {}
  db::Database db;
};

TEST_F(LowCardColumn, Exceeded) {
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load(
      {{"GET"}, {"POST"}, {"HEAD"}, {"PUT"}, {"DELETE"}, {"BLAH"}, {"BLAH"}});

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"http_method"}},
          {"metrics", {"count"}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "1"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"__exceeded", "2"}};
  EXPECT_EQ(expected, output.rows());
}

TEST_F(CardinalityGuard, Exceeded) {
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"13873844", "purchase"},
               {"13873844", "open-app"},
               {"13873844", "close-app"},
               {"13873844", "donate"},
               {"13873844", "click-add"},
               {"13873755", "purchase"},
               {"13873844", "purchase"}});

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"device_id", "event_name"}},
          {"metrics", {"count"}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"13873844", "purchase", "2"},  {"13873844", "open-app", "1"},
      {"13873844", "close-app", "1"}, {"13873844", "__exceeded", "2"},
      {"13873755", "purchase", "1"},
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, DimensionLength) {
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "veryveryveryveryveryveryveryverylongeventname",
                "20141112", "0.1"},
               {"US", "veryveryveryveryvery", "20141112", "0.1"}});

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"event_name"}},
          {"metrics", {"count"}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"veryveryveryveryvery", "2"}};
  EXPECT_EQ(expected, output.rows());
}
