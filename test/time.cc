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
#include "db/rollup.h"
#include "db/store.h"
#include "db/table.h"
#include "query/output.h"
#include <algorithm>
#include <gtest/gtest.h>

namespace util = viya::util;
namespace query = viya::query;

class IngestFormat : public testing::Test {
protected:
  IngestFormat()
      : query_conf(std::move(util::Config(
            json{{"type", "aggregate"},
                 {"table", "events"},
                 {"dimensions", {"country", "install_time"}},
                 {"metrics", {"count"}},
                 {"filter",
                  {{"op", "gt"}, {"column", "count"}, {"value", "0"}}}}))) {}
  util::Config query_conf;
};

TEST_F(IngestFormat, ParseInputFormat) {
  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "events"},
              {"dimensions",
               {{{"name", "country"}},
                {{"name", "install_time"},
                 {"type", "time"},
                 {"format", "%Y-%m-%d %T"}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "2014-11-12 11:25:01"}, {"IL", "2015-11-12 11:00:02"}});

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "1415791501", "1"}, {"IL", "1447326002", "1"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseTime) {
  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "events"},
              {"dimensions",
               {{{"name", "country"}},
                {{"name", "install_time"}, {"type", "time"}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1415791501"}, {"IL", "1447326002"}});

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "1415791501", "1"}, {"IL", "1447326002", "1"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParsePosixTime) {
  db::Database db(std::move(util::Config(json{
      {"tables",
       {{{"name", "events"},
         {"dimensions",
          {{{"name", "country"}},
           {{"name", "install_time"}, {"type", "time"}, {"format", "posix"}}}},
         {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1415791501"}, {"IL", "1447326002"}});

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "1415791501", "1"}, {"IL", "1447326002", "1"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseMillisTime) {
  db::Database db(std::move(util::Config(json{
      {"tables",
       {{{"name", "events"},
         {"dimensions",
          {{{"name", "country"}},
           {{"name", "install_time"}, {"type", "time"}, {"format", "millis"}}}},
         {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1415791501123"}, {"IL", "1447326002819"}});

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "1415791501", "1"}, {"IL", "1447326002", "1"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseMicrosTime) {
  db::Database db(std::move(util::Config(json{
      {"tables",
       {{{"name", "events"},
         {"dimensions",
          {{{"name", "country"}},
           {{"name", "install_time"}, {"type", "time"}, {"format", "micros"}}}},
         {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1415791501123012"}, {"IL", "1447326002819123"}});

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "1415791501", "1"}, {"IL", "1447326002", "1"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParsePosixMicrotime) {
  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "events"},
              {"dimensions",
               {{{"name", "country"}},
                {{"name", "install_time"},
                 {"type", "microtime"},
                 {"format", "posix"}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1415791501"}, {"IL", "1447326002"}});

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "1415791501000000", "1"}, {"IL", "1447326002000000", "1"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseMillisMicrotime) {
  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "events"},
              {"dimensions",
               {{{"name", "country"}},
                {{"name", "install_time"},
                 {"type", "microtime"},
                 {"format", "millis"}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1415791501000"}, {"IL", "1447326002000"}});

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "1415791501000000", "1"}, {"IL", "1447326002000000", "1"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseMicrosMicrotime) {
  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "events"},
              {"dimensions",
               {{{"name", "country"}},
                {{"name", "install_time"},
                 {"type", "microtime"},
                 {"format", "micros"}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1415791501000000"}, {"IL", "1447326002000000"}});

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "1415791501000000", "1"}, {"IL", "1447326002000000", "1"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseMicrotime) {
  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "events"},
              {"dimensions",
               {{{"name", "country"}},
                {{"name", "install_time"}, {"type", "microtime"}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "1415791501000000"}, {"IL", "1447326002000000"}});

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "1415791501000000", "1"}, {"IL", "1447326002000000", "1"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST(IngestGranularity, TruncateToDay) {
  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "events"},
              {"dimensions",
               {{{"name", "country"}},
                {{"name", "install_time"},
                 {"type", "time"},
                 {"format", "%Y-%m-%d %T"},
                 {"granularity", "day"}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "2014-11-12 11:25:01"}, {"IL", "2015-11-12 11:00:02"}});

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"country", "install_time"}},
          {"metrics", {"count"}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "1415750400", "1"}, {"IL", "1447286400", "1"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST(DynamicRollup, TimestampIngestion) {
  setenv("VIYA_TEST_ROLLUP_TS", "1496570140L", 1);

  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "events"},
              {"dimensions",
               {{{"name", "install_time"},
                 {"type", "time"},
                 {"rollup_rules",
                  {{{"granularity", "hour"}, {"after", "1 days"}},
                   {{"granularity", "day"}, {"after", "1 weeks"}},
                   {{"granularity", "month"}, {"after", "1 years"}}}}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");

  std::vector<query::MemoryRowOutput::Row> expected;

  // First day not rolled up:
  input::SimpleLoader loader1(*table);
  loader1.Load({{"1496566539"}, {"1496555739"}, {"1496555700"}});
  expected.push_back({"1496566539", "1"});
  expected.push_back({"1496555739", "1"});
  expected.push_back({"1496555700", "1"});

  // Rollup by hour after 1 day:
  input::SimpleLoader loader2(*table);
  loader2.Load({{"1496408066"}, {"1496405460"}, {"1496315533"}});
  expected.push_back({"1496404800", "2"});
  expected.push_back({"1496314800", "1"});

  // Rollup by day after 1 week:
  input::SimpleLoader loader3(*table);
  loader3.Load({{"1495948331"}, {"1495941131"}, {"1495854731"}});
  expected.push_back({"1495929600", "2"});
  expected.push_back({"1495843200", "1"});

  // Rollup by month after 1 year:
  input::SimpleLoader loader4(*table);
  loader4.Load({{"1461801600"}, {"1422403212"}, {"1421153666"}});
  expected.push_back({"1420070400", "2"});
  expected.push_back({"1459468800", "1"});

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"install_time"}},
          {"metrics", {"count"}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}}})),
      output);

  auto actual = output.rows();
  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
  EXPECT_EQ(1, table->store()->segments().size());
  EXPECT_EQ(expected.size(), table->store()->segments()[0]->size());
}

TEST(DynamicRollup, TimestampMicroIngestion) {
  setenv("VIYA_TEST_ROLLUP_TS", "1496570140L", 1);

  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "events"},
              {"dimensions",
               {{{"name", "install_time"},
                 {"type", "microtime"},
                 {"rollup_rules",
                  {{{"granularity", "hour"}, {"after", "1 days"}},
                   {{"granularity", "day"}, {"after", "1 weeks"}},
                   {{"granularity", "month"}, {"after", "1 years"}}}}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");

  std::vector<query::MemoryRowOutput::Row> expected;

  // First day not rolled up:
  input::SimpleLoader loader1(*table);
  loader1.Load(
      {{"1496566539124818"}, {"1496555739100001"}, {"1496555700172000"}});
  expected.push_back({"1496566539124818", "1"});
  expected.push_back({"1496555739100001", "1"});
  expected.push_back({"1496555700172000", "1"});

  // Rollup by hour after 1 day:
  input::SimpleLoader loader2(*table);
  loader2.Load(
      {{"1496408066102001"}, {"1496405460111113"}, {"1496315533371291"}});
  expected.push_back({"1496404800000000", "2"});
  expected.push_back({"1496314800000000", "1"});

  // Rollup by day after 1 week:
  input::SimpleLoader loader3(*table);
  loader3.Load(
      {{"1495948331000000"}, {"1495941131010826"}, {"1495854731192710"}});
  expected.push_back({"1495929600000000", "2"});
  expected.push_back({"1495843200000000", "1"});

  // Rollup by month after 1 year:
  input::SimpleLoader loader4(*table);
  loader4.Load(
      {{"1461801600000100"}, {"1422403212128711"}, {"1421153666182700"}});
  expected.push_back({"1420070400000000", "2"});
  expected.push_back({"1459468800000000", "1"});

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"install_time"}},
          {"metrics", {"count"}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}}})),
      output);

  auto actual = output.rows();
  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
  EXPECT_EQ(1, table->store()->segments().size());
  EXPECT_EQ(expected.size(), table->store()->segments()[0]->size());
}

TEST(DynamicRollup, FormatIngestion) {
  setenv("VIYA_TEST_ROLLUP_TS", "1496570140L", 1);

  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "events"},
              {"dimensions",
               {{{"name", "install_time"},
                 {"type", "time"},
                 {"format", "%Y-%m-%d %T"},
                 {"rollup_rules",
                  {{{"granularity", "minute"}, {"after", "1 hours"}},
                   {{"granularity", "hour"}, {"after", "1 days"}},
                   {{"granularity", "day"}, {"after", "1 months"}},
                   {{"granularity", "month"}, {"after", "1 years"}}}}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("events");

  std::vector<query::MemoryRowOutput::Row> expected;

  // First hour is not rolled up:
  input::SimpleLoader loader1(*table);
  loader1.Load({{"2017-06-04 09:55:50"},
                {"2017-06-04 10:11:01"},
                {"2017-06-04 10:54:10"}});
  expected.push_back({"2017-06-04 09:55:50", "1"});
  expected.push_back({"2017-06-04 10:11:01", "1"});
  expected.push_back({"2017-06-04 10:54:10", "1"});

  // Rollup by minute after 1 hour:
  input::SimpleLoader loader2(*table);
  loader2.Load({{"2017-06-04 08:55:39"},
                {"2017-06-04 08:55:30"},
                {"2017-06-04 07:15:10"}});
  expected.push_back({"2017-06-04 08:55:00", "2"});
  expected.push_back({"2017-06-04 07:15:00", "1"});

  // Rollup by hour after 1 day:
  input::SimpleLoader loader3(*table);
  loader3.Load({{"2017-06-03 08:14:13"},
                {"2017-06-02 18:13:22"},
                {"2017-06-02 18:24:10"}});
  expected.push_back({"2017-06-02 18:00:00", "2"});
  expected.push_back({"2017-06-03 08:00:00", "1"});

  // Rollup by day after 1 month:
  input::SimpleLoader loader4(*table);
  loader4.Load({{"2017-05-03 12:14:05"},
                {"2017-05-02 08:41:45"},
                {"2017-05-03 15:25:32"}});
  expected.push_back({"2017-05-03 00:00:00", "2"});
  expected.push_back({"2017-05-02 00:00:00", "1"});

  // Rollup by month after 1 year:
  input::SimpleLoader loader5(*table);
  loader5.Load({{"2016-02-03 18:21:09"},
                {"2012-06-03 12:31:57"},
                {"2016-02-04 04:44:45"}});
  expected.push_back({"2016-02-01 00:00:00", "2"});
  expected.push_back({"2012-06-01 00:00:00", "1"});

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"select",
           {{{"column", "install_time"}, {"format", "%Y-%m-%d %T"}},
            {{"column", "count"}}}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}}})),
      output);

  auto actual = output.rows();
  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
  EXPECT_EQ(1, table->store()->segments().size());
  EXPECT_EQ(expected.size(), table->store()->segments()[0]->size());
}

TEST_F(TimeEvents, OutputFormat) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"select",
           {{{"column", "country"}},
            {{"column", "install_time"}, {"format", "%Y-%m-%d %T"}},
            {{"column", "count"}}}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"IL", "2015-01-03 12:13:14", "1"}, {"KZ", "2015-01-03 04:00:00", "1"},
      {"KZ", "2015-01-05 20:00:00", "1"}, {"RU", "2015-01-01 22:11:24", "1"},
      {"US", "2015-01-01 10:11:24", "1"}, {"US", "2015-01-02 05:55:11", "1"},
      {"US", "2015-01-05 22:10:05", "1"}, {"US", "2015-01-07 10:05:32", "1"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(TimeEvents, QueryGranularity) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"select",
           {{{"column", "country"}},
            {{"column", "install_time"},
             {"format", "%Y-%m-%d %T"},
             {"granularity", "month"}},
            {{"column", "count"}}}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"IL", "2015-01-01 00:00:00", "1"},
      {"KZ", "2015-01-01 00:00:00", "2"},
      {"RU", "2015-01-01 00:00:00", "1"},
      {"US", "2015-01-01 00:00:00", "4"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(TimeGranularitiesEvents, IngestRollupYear) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(
               json{{"type", "aggregate"},
                    {"table", "events"},
                    {"select", {{{"column", "t1"}}, {{"column", "count"}}}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"2000-01-01 00:00:00", "4"}, {"2001-01-01 00:00:00", "4"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(TimeGranularitiesEvents, IngestRollupMonth) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(
               json{{"type", "aggregate"},
                    {"table", "events"},
                    {"select", {{{"column", "t2"}}, {{"column", "count"}}}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"2001-01-01 00:00:00", "4"}, {"2001-02-01 00:00:00", "4"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(TimeGranularitiesEvents, IngestRollupDay) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(
               json{{"type", "aggregate"},
                    {"table", "events"},
                    {"select", {{{"column", "t3"}}, {{"column", "count"}}}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"2003-03-03 00:00:00", "4"}, {"2003-03-04 00:00:00", "4"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(TimeGranularitiesEvents, IngestRollupHour) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(
               json{{"type", "aggregate"},
                    {"table", "events"},
                    {"select", {{{"column", "t4"}}, {{"column", "count"}}}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"2004-04-04 04:00:00", "4"}, {"2004-04-04 05:00:00", "4"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(TimeGranularitiesEvents, IngestRollupMinute) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(
               json{{"type", "aggregate"},
                    {"table", "events"},
                    {"select", {{{"column", "t5"}}, {{"column", "count"}}}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"2005-05-05 05:05:00", "4"}, {"2005-05-05 05:06:00", "4"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(TimeGranularitiesEvents, IngestRollupSecond) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(
               json{{"type", "aggregate"},
                    {"table", "events"},
                    {"select", {{{"column", "t6"}}, {{"column", "count"}}}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"2006-06-06 06:06:06", "4"}, {"2006-06-06 06:06:07", "4"}};
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}
