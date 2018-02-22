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

using SearchEvents = InappEvents;

TEST_F(SearchEvents, SearchQuery) {
  LoadSearchEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{
               {"type", "search"},
               {"table", "events"},
               {"dimension", "event_name"},
               {"term", "r"},
               {"limit", 10},
               {"filter",
                {{"op", "gt"}, {"column", "revenue"}, {"value", "1.0"}}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"refund", "review"}};

  auto actual = output.rows();
  std::sort(actual[0].begin(), actual[0].end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SearchEvents, QueryWithHeader) {
  LoadSearchEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{
               {"type", "search"},
               {"header", true},
               {"table", "events"},
               {"dimension", "event_name"},
               {"term", "ref"},
               {"filter",
                {{"op", "gt"}, {"column", "revenue"}, {"value", "1.0"}}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"event_name"},
                                                       {"refund"}};

  auto actual = output.rows();

  EXPECT_EQ(expected, actual);
}

TEST_F(SearchEvents, SearchQueryLimit) {
  LoadSearchEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{
               {"type", "search"},
               {"table", "events"},
               {"dimension", "event_name"},
               {"term", ""},
               {"limit", 2},
               {"filter",
                {{"op", "gt"}, {"column", "install_time"}, {"value", "0"}}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"purchase", "refund"}};

  auto actual = output.rows();
  std::sort(actual[0].begin(), actual[0].end());

  EXPECT_EQ(expected, actual);
}
