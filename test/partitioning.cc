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

TEST_F(MultiTenantEvents, PartitionBySingleColumn) {
  LoadEvents(util::Config(json{
      {"partition_filter",
       {{"columns", {"app_id"}}, {"values", {0}}, {"total_partitions", 5}}}}));

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{{"type", "aggregate"},
                                       {"table", "events"},
                                       {"dimensions", {"app_id"}},
                                       {"metrics", {"count"}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"com.horse", "3"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(MultiTenantEvents, PartitionByMultiColumn) {
  LoadEvents(util::Config(json{{"partition_filter",
                                {{"columns", {"app_id", "country"}},
                                 {"values", {0}},
                                 {"total_partitions", 5}}}}));

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{{"type", "aggregate"},
                                       {"table", "events"},
                                       {"dimensions", {"app_id", "country"}},
                                       {"metrics", {"count"}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"com.bird", "KZ", "1"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}
