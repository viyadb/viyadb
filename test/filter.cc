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

TEST_F(InappEvents, FilterNot) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config("{\"type\": \"aggregate\","
                             " \"table\": \"events\","
                             " \"dimensions\": [\"event_name\", \"country\"],"
                             " \"metrics\": [\"revenue\"],"
                             " \"filter\": {\"op\": \"not\", \"filter\":"
                             "              {\"op\": \"ne\", \"column\": "
                             "\"country\", \"value\": \"US\"}}}")),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"purchase", "US", "1.2"}, {"donate", "US", "5"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, FilterNotOrNe) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config("{\"type\": \"aggregate\","
                             " \"table\": \"events\","
                             " \"dimensions\": [\"event_name\", \"country\"],"
                             " \"metrics\": [\"revenue\"],"
                             " \"filter\": {\"op\": \"not\", \"filter\":"
                             "              {\"op\": \"or\", \"filters\": ["
                             "               {\"op\": \"ne\", \"column\": "
                             "\"country\", \"value\": \"CH\"},"
                             "               {\"op\": \"ne\", \"column\": "
                             "\"event_name\", \"value\": \"refund\"}]}}}")),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"refund", "CH", "1.1"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, FilterNotPeriod) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config("{\"type\": \"aggregate\","
                             " \"table\": \"events\","
                             " \"dimensions\": [\"event_name\", \"country\"],"
                             " \"metrics\": [\"revenue\"],"
                             " \"filter\": {\"op\": \"not\", \"filter\":"
                             "              {\"op\": \"and\", \"filters\": ["
                             "               {\"op\": \"gt\", \"column\": "
                             "\"revenue\", \"value\": \"0.1\"},"
                             "               {\"op\": \"lt\", \"column\": "
                             "\"revenue\", \"value\": \"2\"}]}}}")),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"purchase", "US", "0.1"}, {"review", "KZ", "5"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, FilterNotIn) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(
               "{\"type\": \"aggregate\","
               " \"table\": \"events\","
               " \"dimensions\": [\"event_name\", \"country\"],"
               " \"metrics\": [\"revenue\"],"
               " \"filter\": {\"op\": \"not\", \"filter\":"
               "              {\"op\": \"in\", \"column\": \"event_name\", "
               "\"values\": [\"refund\", \"purchase\"]}}}")),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"donate", "RU", "1"},
                                                       {"review", "KZ", "5"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}
