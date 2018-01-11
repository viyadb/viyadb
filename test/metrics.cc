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
#include <algorithm>
#include <gtest/gtest.h>

namespace db = viya::db;
namespace util = viya::util;
namespace query = viya::query;
namespace input = viya::input;

class MetricEventsByte : public testing::Test {
protected:
  MetricEventsByte()
      : db(std::move(util::Config(
            "{\"tables\": [{\"name\": \"events\","
            "               \"dimensions\": [{\"name\": \"country\"}],"
            "               \"metrics\": [{\"name\": \"count\", \"type\": "
            "\"count\"},"
            "                             {\"name\": \"byte_sum\", \"type\": "
            "\"byte_sum\"},"
            "                             {\"name\": \"byte_min\", \"type\": "
            "\"byte_min\"},"
            "                             {\"name\": \"byte_max\", \"type\": "
            "\"byte_max\"},"
            "                             {\"name\": \"byte_avg\", \"type\": "
            "\"byte_avg\"},"
            "                             {\"name\": \"ubyte_sum\", \"type\": "
            "\"ubyte_sum\"},"
            "                             {\"name\": \"ubyte_min\", \"type\": "
            "\"ubyte_min\"},"
            "                             {\"name\": \"ubyte_max\", \"type\": "
            "\"ubyte_max\"},"
            "                             {\"name\": \"ubyte_avg\", \"type\": "
            "\"ubyte_avg\"}]}]}"))) {}
  db::Database db;
};

class MetricEventsShort : public testing::Test {
protected:
  MetricEventsShort()
      : db(std::move(util::Config(
            "{\"tables\": [{\"name\": \"events\","
            "               \"dimensions\": [{\"name\": \"country\"}],"
            "               \"metrics\": [{\"name\": \"count\", \"type\": "
            "\"count\"},"
            "                             {\"name\": \"short_sum\", \"type\": "
            "\"short_sum\"},"
            "                             {\"name\": \"short_min\", \"type\": "
            "\"short_min\"},"
            "                             {\"name\": \"short_max\", \"type\": "
            "\"short_max\"},"
            "                             {\"name\": \"short_avg\", \"type\": "
            "\"short_avg\"},"
            "                             {\"name\": \"ushort_sum\", \"type\": "
            "\"ushort_sum\"},"
            "                             {\"name\": \"ushort_min\", \"type\": "
            "\"ushort_min\"},"
            "                             {\"name\": \"ushort_max\", \"type\": "
            "\"ushort_max\"},"
            "                             {\"name\": \"ushort_avg\", \"type\": "
            "\"ushort_avg\"}]}]}"))) {}
  db::Database db;
};

class MetricEventsInt : public testing::Test {
protected:
  MetricEventsInt()
      : db(std::move(util::Config(
            "{\"tables\": [{\"name\": \"events\","
            "               \"dimensions\": [{\"name\": \"country\"}],"
            "               \"metrics\": [{\"name\": \"count\", \"type\": "
            "\"count\"},"
            "                             {\"name\": \"int_sum\", \"type\": "
            "\"int_sum\"},"
            "                             {\"name\": \"int_min\", \"type\": "
            "\"int_min\"},"
            "                             {\"name\": \"int_max\", \"type\": "
            "\"int_max\"},"
            "                             {\"name\": \"int_avg\", \"type\": "
            "\"int_avg\"},"
            "                             {\"name\": \"uint_sum\", \"type\": "
            "\"uint_sum\"},"
            "                             {\"name\": \"uint_min\", \"type\": "
            "\"uint_min\"},"
            "                             {\"name\": \"uint_max\", \"type\": "
            "\"uint_max\"},"
            "                             {\"name\": \"uint_avg\", \"type\": "
            "\"uint_avg\"}]}]}"))) {}
  db::Database db;
};

class MetricEventsLong : public testing::Test {
protected:
  MetricEventsLong()
      : db(std::move(util::Config(
            "{\"tables\": [{\"name\": \"events\","
            "               \"dimensions\": [{\"name\": \"country\"}],"
            "               \"metrics\": [{\"name\": \"count\", \"type\": "
            "\"count\"},"
            "                             {\"name\": \"long_sum\", \"type\": "
            "\"long_sum\"},"
            "                             {\"name\": \"long_min\", \"type\": "
            "\"long_min\"},"
            "                             {\"name\": \"long_max\", \"type\": "
            "\"long_max\"},"
            "                             {\"name\": \"long_avg\", \"type\": "
            "\"long_avg\"},"
            "                             {\"name\": \"ulong_sum\", \"type\": "
            "\"ulong_sum\"},"
            "                             {\"name\": \"ulong_min\", \"type\": "
            "\"ulong_min\"},"
            "                             {\"name\": \"ulong_max\", \"type\": "
            "\"ulong_max\"},"
            "                             {\"name\": \"ulong_avg\", \"type\": "
            "\"ulong_avg\"}]}]}"))) {}
  db::Database db;
};

class MetricEventsFloat : public testing::Test {
protected:
  MetricEventsFloat()
      : db(std::move(util::Config(
            "{\"tables\": [{\"name\": \"events\","
            "               \"dimensions\": [{\"name\": \"country\"}],"
            "               \"metrics\": [{\"name\": \"count\", \"type\": "
            "\"count\"},"
            "                             {\"name\": \"float_sum\", \"type\": "
            "\"float_sum\"},"
            "                             {\"name\": \"float_min\", \"type\": "
            "\"float_min\"},"
            "                             {\"name\": \"float_max\", \"type\": "
            "\"float_max\"},"
            "                             {\"name\": \"float_avg\", \"type\": "
            "\"float_avg\"},"
            "                             {\"name\": \"double_sum\", \"type\": "
            "\"double_sum\"},"
            "                             {\"name\": \"double_min\", \"type\": "
            "\"double_min\"},"
            "                             {\"name\": \"double_max\", \"type\": "
            "\"double_max\"},"
            "                             {\"name\": \"double_avg\", \"type\": "
            "\"double_avg\"}]}]}"))) {}
  db::Database db;
};

TEST_F(MetricEventsByte, AggregateMetrics) {
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1", "1", "1", "1", "1", "1", "1", "1", "1"},
               {"US", "2", "2", "2", "2", "2", "2", "2", "2", "2"},
               {"US", "3", "3", "3", "3", "3", "3", "3", "3", "3"},
               {"IL", "1", "1", "1", "1", "1", "1", "1", "1", "1"},
               {"IL", "2", "2", "2", "2", "2", "2", "2", "2", "2"}});

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config("{\"type\": \"aggregate\","
                                  " \"table\": \"events\","
                                  " \"dimensions\": [\"country\"],"
                                  " \"metrics\": [\"count\","
                                  "               \"byte_sum\","
                                  "               \"byte_min\","
                                  "               \"byte_max\","
                                  "               \"byte_avg\","
                                  "               \"ubyte_sum\","
                                  "               \"ubyte_min\","
                                  "               \"ubyte_max\","
                                  "               \"ubyte_avg\"],"
                                  " \"filter\": {\"op\": \"ge\", \"column\": "
                                  "\"count\", \"value\": \"1\"}}")),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "3", "6", "1", "3", "2", "6", "1", "3", "2"},
      {"IL", "2", "3", "1", "2", "1.5", "3", "1", "2", "1.5"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(MetricEventsShort, AggregateMetrics) {
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1", "1", "1", "1", "1", "1", "1", "1", "1"},
               {"US", "2", "2", "2", "2", "2", "2", "2", "2", "2"},
               {"US", "3", "3", "3", "3", "3", "3", "3", "3", "3"},
               {"IL", "1", "1", "1", "1", "1", "1", "1", "1", "1"},
               {"IL", "2", "2", "2", "2", "2", "2", "2", "2", "2"}});

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config("{\"type\": \"aggregate\","
                                  " \"table\": \"events\","
                                  " \"dimensions\": [\"country\"],"
                                  " \"metrics\": [\"count\","
                                  "               \"short_sum\","
                                  "               \"short_min\","
                                  "               \"short_max\","
                                  "               \"short_avg\","
                                  "               \"ushort_sum\","
                                  "               \"ushort_min\","
                                  "               \"ushort_max\","
                                  "               \"ushort_avg\"],"
                                  " \"filter\": {\"op\": \"ge\", \"column\": "
                                  "\"count\", \"value\": \"1\"}}")),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "3", "6", "1", "3", "2", "6", "1", "3", "2"},
      {"IL", "2", "3", "1", "2", "1.5", "3", "1", "2", "1.5"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(MetricEventsInt, AggregateMetrics) {
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1", "1", "1", "1", "1", "1", "1", "1", "1"},
               {"US", "2", "2", "2", "2", "2", "2", "2", "2", "2"},
               {"US", "3", "3", "3", "3", "3", "3", "3", "3", "3"},
               {"IL", "1", "1", "1", "1", "1", "1", "1", "1", "1"},
               {"IL", "2", "2", "2", "2", "2", "2", "2", "2", "2"}});

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config("{\"type\": \"aggregate\","
                                  " \"table\": \"events\","
                                  " \"dimensions\": [\"country\"],"
                                  " \"metrics\": [\"count\","
                                  "               \"int_sum\","
                                  "               \"int_min\","
                                  "               \"int_max\","
                                  "               \"int_avg\","
                                  "               \"uint_sum\","
                                  "               \"uint_min\","
                                  "               \"uint_max\","
                                  "               \"uint_avg\"],"
                                  " \"filter\": {\"op\": \"ge\", \"column\": "
                                  "\"count\", \"value\": \"1\"}}")),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "3", "6", "1", "3", "2", "6", "1", "3", "2"},
      {"IL", "2", "3", "1", "2", "1.5", "3", "1", "2", "1.5"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(MetricEventsLong, AggregateMetrics) {
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1", "1", "1", "1", "1", "1", "1", "1", "1"},
               {"US", "2", "2", "2", "2", "2", "2", "2", "2", "2"},
               {"US", "3", "3", "3", "3", "3", "3", "3", "3", "3"},
               {"IL", "1", "1", "1", "1", "1", "1", "1", "1", "1"},
               {"IL", "2", "2", "2", "2", "2", "2", "2", "2", "2"}});

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config("{\"type\": \"aggregate\","
                                  " \"table\": \"events\","
                                  " \"dimensions\": [\"country\"],"
                                  " \"metrics\": [\"count\","
                                  "               \"long_sum\","
                                  "               \"long_min\","
                                  "               \"long_max\","
                                  "               \"long_avg\","
                                  "               \"ulong_sum\","
                                  "               \"ulong_min\","
                                  "               \"ulong_max\","
                                  "               \"ulong_avg\"],"
                                  " \"filter\": {\"op\": \"ge\", \"column\": "
                                  "\"count\", \"value\": \"1\"}}")),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "3", "6", "1", "3", "2", "6", "1", "3", "2"},
      {"IL", "2", "3", "1", "2", "1.5", "3", "1", "2", "1.5"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(MetricEventsFloat, AggregateMetrics) {
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1", "1", "1", "1", "1", "1", "1", "1", "1"},
               {"US", "2", "2", "2", "2", "2", "2", "2", "2", "2"},
               {"US", "3", "3", "3", "3", "3", "3", "3", "3", "3"},
               {"IL", "1", "1", "1", "1", "1", "1", "1", "1", "1"},
               {"IL", "2", "2", "2", "2", "2", "2", "2", "2", "2"}});

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config("{\"type\": \"aggregate\","
                                  " \"table\": \"events\","
                                  " \"dimensions\": [\"country\"],"
                                  " \"metrics\": [\"count\","
                                  "               \"float_sum\","
                                  "               \"float_min\","
                                  "               \"float_max\","
                                  "               \"float_avg\","
                                  "               \"double_sum\","
                                  "               \"double_min\","
                                  "               \"double_max\","
                                  "               \"double_avg\"],"
                                  " \"filter\": {\"op\": \"ge\", \"column\": "
                                  "\"count\", \"value\": \"1\"}}")),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "3", "6", "1", "3", "2", "6", "1", "3", "2"},
      {"IL", "2", "3", "1", "2", "1.5", "3", "1", "2", "1.5"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST(MetricEvents, AverageWithoutCount) {
  db::Database db(std::move(
      util::Config("{\"tables\": [{\"name\": \"events\","
                   "               \"dimensions\": [{\"name\": \"country\"}],"
                   "               \"metrics\": [{\"name\": \"avg_revenue\", "
                   "\"type\": \"double_avg\"}]}]}")));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load(
      {{"US", "16"}, {"US", "6"}, {"US", "2.6"}, {"IL", "20.7"}, {"IL", "11"}});

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config("{\"type\": \"aggregate\","
                                  " \"table\": \"events\","
                                  " \"dimensions\": [\"country\"],"
                                  " \"metrics\": [\"avg_revenue\"],"
                                  " \"filter\": {\"op\": \"ge\", \"column\": "
                                  "\"avg_revenue\", \"value\": \"1\"}}")),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"US", "8.2"},
                                                       {"IL", "15.85"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST(MetricEvents, AverageQueryRollup) {
  db::Database db(std::move(
      util::Config("{\"tables\": [{\"name\": \"events\","
                   "               \"dimensions\": [{\"name\": \"country\"}],"
                   "               \"metrics\": [{\"name\": \"avg_revenue\", "
                   "\"type\": \"double_avg\"}]}]}")));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load(
      {{"US", "16"}, {"US", "6"}, {"US", "2.6"}, {"IL", "20.7"}, {"IL", "11"}});

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config("{\"type\": \"aggregate\","
                                  " \"table\": \"events\","
                                  " \"dimensions\": [],"
                                  " \"metrics\": [\"avg_revenue\"],"
                                  " \"filter\": {\"op\": \"ge\", \"column\": "
                                  "\"avg_revenue\", \"value\": \"1\"}}")),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"11.26"}};
  EXPECT_EQ(expected, output.rows());
}
