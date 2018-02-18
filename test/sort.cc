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

using SortEvents = InappEvents;

TEST_F(SortEvents, PaginateNoSort) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{
               {"type", "aggregate"},
               {"table", "events"},
               {"dimensions", {"country", "event_name"}},
               {"metrics", {"count"}},
               {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}},
               {"skip", 1},
               {"limit", 2}})),
           output);

  EXPECT_EQ(2, output.rows().size());
}

TEST_F(SortEvents, SortMultiColumn) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{
               {"type", "aggregate"},
               {"table", "events"},
               {"dimensions", {"country", "event_name"}},
               {"metrics", {"revenue"}},
               {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}},
               {"sort",
                {{{"column", "revenue"}, {"ascending", false}},
                 {{"column", "country"}, {"ascending", true}}}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"KZ", "review", "5"},   {"AZ", "refund", "1.1"},
      {"CH", "refund", "1.1"}, {"IL", "refund", "1.01"},
      {"RU", "donate", "1"},   {"US", "purchase", "0.1"}};

  EXPECT_EQ(expected, output.rows());
}

TEST_F(SortEvents, TopN) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{
               {"type", "aggregate"},
               {"table", "events"},
               {"dimensions", {"country"}},
               {"metrics", {"revenue"}},
               {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}},
               {"sort",
                {{{"column", "revenue"}},
                 {{"column", "country"}, {"ascending", true}}}},
               {"limit", 5}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"KZ", "5"}, {"AZ", "1.1"}, {"CH", "1.1"}, {"IL", "1.01"}, {"RU", "1"},
  };

  EXPECT_EQ(expected, output.rows());
}

TEST_F(SortEvents, LimitGreaterThanResult) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{
               {"type", "aggregate"},
               {"table", "events"},
               {"dimensions", {"country"}},
               {"metrics", {"revenue"}},
               {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}},
               {"sort",
                {{{"column", "revenue"}},
                 {{"column", "country"}, {"ascending", true}}}},
               {"limit", 50}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"KZ", "5"},    {"AZ", "1.1"}, {"CH", "1.1"},
      {"IL", "1.01"}, {"RU", "1"},   {"US", "0.1"}};

  EXPECT_EQ(expected, output.rows());
}
