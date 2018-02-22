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
#include "query/output.h"
#include "util/config.h"
#include <algorithm>
#include <gtest/gtest.h>

namespace util = viya::util;
namespace query = viya::query;

using SelectEvents = InappEvents;

TEST_F(SelectEvents, BasicQuery) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "select"},
          {"table", "events"},
          {"dimensions", {"event_name", "country"}},
          {"metrics", {"revenue"}},
          {"filter", {{"op", "eq"}, {"column", "country"}, {"value", "US"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"purchase", "US", "0.1"},
      {"purchase", "US", "1.1"},
      {"donate", "US", "5"}};

  auto actual = output.rows();

  EXPECT_EQ(expected, actual);
}

TEST_F(SelectEvents, QueryWithHeader) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "select"},
          {"header", true},
          {"table", "events"},
          {"dimensions", {"country"}},
          {"metrics", {"revenue"}},
          {"filter", {{"op", "eq"}, {"column", "country"}, {"value", "US"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"country", "revenue"}, {"US", "0.1"}, {"US", "1.1"}, {"US", "5"}};

  auto actual = output.rows();

  EXPECT_EQ(expected, actual);
}

TEST_F(SelectEvents, QueryWithLimit) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{{"type", "select"},
                                  {"table", "events"},
                                  {"dimensions", {"event_name", "country"}},
                                  {"metrics", {"revenue"}},
                                  {"limit", 2}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"purchase", "US", "0.1"}, {"purchase", "US", "1.1"}};

  auto actual = output.rows();

  EXPECT_EQ(expected, actual);
}

TEST_F(SelectEvents, QueryWithSkip) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{{"type", "select"},
                                  {"table", "events"},
                                  {"dimensions", {"event_name", "country"}},
                                  {"metrics", {"revenue"}},
                                  {"skip", 2}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"donate", "US", "5"}};

  auto actual = output.rows();

  EXPECT_EQ(expected, actual);
}
