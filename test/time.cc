#include <algorithm>
#include <ctime>
#include "util/config.h"
#include "db/database.h"
#include "db/rollup.h"
#include "query/output.h"
#include "gtest/gtest.h"

namespace db = viya::db;
namespace util = viya::util;
namespace query = viya::query;

class IngestFormat : public testing::Test {
  protected:
    IngestFormat()
      :query_conf(
        std::move(util::Config(
            "{\"type\": \"aggregate\","
            " \"table\": \"events\","
            " \"dimensions\": [\"country\", \"install_time\"],"
            " \"metrics\": [\"count\"],"
            " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"}}"))) {}
    util::Config query_conf;
};

TEST_F(IngestFormat, ParseInputFormat)
{
  db::Database db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"install_time\","
              "                                 \"type\": \"time\","
              "                                 \"format\": \"%Y-%m-%d %T\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "2014-11-12 11:25:01"},
    {"IL", "2015-11-12 11:00:02"}
  });

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "1415791501", "1"},
    {"IL", "1447326002", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseTime)
{
  db::Database db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"install_time\","
              "                                 \"type\": \"time\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "1415791501"},
    {"IL", "1447326002"}
  });

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "1415791501", "1"},
    {"IL", "1447326002", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParsePosixTime)
{
  db::Database db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"install_time\","
              "                                 \"type\": \"time\","
              "                                 \"format\": \"posix\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "1415791501"},
    {"IL", "1447326002"}
  });

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "1415791501", "1"},
    {"IL", "1447326002", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseMillisTime)
{
  db::Database db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"install_time\","
              "                                 \"type\": \"time\","
              "                                 \"format\": \"millis\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "1415791501123"},
    {"IL", "1447326002819"}
  });

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "1415791501", "1"},
    {"IL", "1447326002", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseMicrosTime)
{
  db::Database db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"install_time\","
              "                                 \"type\": \"time\","
              "                                 \"format\": \"micros\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "1415791501123012"},
    {"IL", "1447326002819123"}
  });

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "1415791501", "1"},
    {"IL", "1447326002", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParsePosixMicrotime)
{
  db::Database db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"install_time\","
              "                                 \"type\": \"microtime\","
              "                                 \"format\": \"posix\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "1415791501"},
    {"IL", "1447326002"}
  });

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "1415791501000000", "1"},
    {"IL", "1447326002000000", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseMillisMicrotime)
{
  db::Database db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"install_time\","
              "                                 \"type\": \"microtime\","
              "                                 \"format\": \"millis\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "1415791501000"},
    {"IL", "1447326002000"}
  });

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "1415791501000000", "1"},
    {"IL", "1447326002000000", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseMicrosMicrotime)
{
  db::Database db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"install_time\","
              "                                 \"type\": \"microtime\","
              "                                 \"format\": \"micros\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "1415791501000000"},
    {"IL", "1447326002000000"}
  });

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "1415791501000000", "1"},
    {"IL", "1447326002000000", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(IngestFormat, ParseMicrotime)
{
  db::Database db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"install_time\","
              "                                 \"type\": \"microtime\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "1415791501000000"},
    {"IL", "1447326002000000"}
  });

  query::MemoryRowOutput output;
  db.Query(query_conf, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "1415791501000000", "1"},
    {"IL", "1447326002000000", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST(IngestGranularity, TruncateToDay)
{
  db::Database db(std::move(util::Config(
        "{\"tables\": [{\"name\": \"events\","

        "               \"dimensions\": [{\"name\": \"country\"},"
        "                                {\"name\": \"install_time\","
        "                                 \"type\": \"time\","
        "                                 \"format\": \"%Y-%m-%d %T\","
        "                                 \"granularity\": \"day\"}],"
        "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "2014-11-12 11:25:01"},
    {"IL", "2015-11-12 11:00:02"}
  });

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"country\", \"install_time\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "1415750400", "1"},
    {"IL", "1447286400", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST(DynamicRollup, TimestampIngestion)
{
  setenv("VIYA_TEST_ROLLUP_TS", "1496570140L", 1);

  db::Database db(std::move(util::Config(
        "{\"tables\": [{\"name\": \"events\","
        "               \"dimensions\": [{\"name\": \"install_time\","
        "                                 \"type\": \"time\","
        "                                 \"rollup_rules\": ["
        "                                   {\"granularity\": \"hour\",  \"after\": \"1 days\"},"
        "                                   {\"granularity\": \"day\",   \"after\": \"1 weeks\"},"
        "                                   {\"granularity\": \"month\", \"after\": \"1 years\"}"
        "                                ]}],"
        "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");

  std::vector<query::MemoryRowOutput::Row> expected;

  // First day not rolled up:
  table->Load({
    {"1496566539"},
    {"1496555739"},
    {"1496555700"}
  });
  expected.push_back({"1496566539", "1"});
  expected.push_back({"1496555739", "1"});
  expected.push_back({"1496555700", "1"});

  // Rollup by hour after 1 day:
  table->Load({
    {"1496408066"},
    {"1496405460"},
    {"1496315533"}
  });
  expected.push_back({"1496404800", "2"});
  expected.push_back({"1496314800", "1"});

  // Rollup by day after 1 week:
  table->Load({
    {"1495948331"},
    {"1495941131"},
    {"1495854731"}
  });
  expected.push_back({"1495929600", "2"});
  expected.push_back({"1495843200", "1"});

  // Rollup by month after 1 year:
  table->Load({
    {"1461801600"},
    {"1422403212"},
    {"1421153666"}
  });
  expected.push_back({"1420070400", "2"});
  expected.push_back({"1459468800", "1"});

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"install_time\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"}}")), output);

  auto actual = output.rows();
  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
  EXPECT_EQ(1, table->store()->segments().size());
  EXPECT_EQ(expected.size(), table->store()->segments()[0]->size());
}

TEST(DynamicRollup, TimestampMicroIngestion)
{
  setenv("VIYA_TEST_ROLLUP_TS", "1496570140L", 1);

  db::Database db(std::move(util::Config(
        "{\"tables\": [{\"name\": \"events\","
        "               \"dimensions\": [{\"name\": \"install_time\","
        "                                 \"type\": \"microtime\","
        "                                 \"rollup_rules\": ["
        "                                   {\"granularity\": \"hour\",  \"after\": \"1 days\"},"
        "                                   {\"granularity\": \"day\",   \"after\": \"1 weeks\"},"
        "                                   {\"granularity\": \"month\", \"after\": \"1 years\"}"
        "                                ]}],"
        "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");

  std::vector<query::MemoryRowOutput::Row> expected;

  // First day not rolled up:
  table->Load({
    {"1496566539124818"},
    {"1496555739100001"},
    {"1496555700172000"}
  });
  expected.push_back({"1496566539124818", "1"});
  expected.push_back({"1496555739100001", "1"});
  expected.push_back({"1496555700172000", "1"});

  // Rollup by hour after 1 day:
  table->Load({
    {"1496408066102001"},
    {"1496405460111113"},
    {"1496315533371291"}
  });
  expected.push_back({"1496404800000000", "2"});
  expected.push_back({"1496314800000000", "1"});

  // Rollup by day after 1 week:
  table->Load({
    {"1495948331000000"},
    {"1495941131010826"},
    {"1495854731192710"}
  });
  expected.push_back({"1495929600000000", "2"});
  expected.push_back({"1495843200000000", "1"});

  // Rollup by month after 1 year:
  table->Load({
    {"1461801600000100"},
    {"1422403212128711"},
    {"1421153666182700"}
  });
  expected.push_back({"1420070400000000", "2"});
  expected.push_back({"1459468800000000", "1"});

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"install_time\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"}}")), output);

  auto actual = output.rows();
  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
  EXPECT_EQ(1, table->store()->segments().size());
  EXPECT_EQ(expected.size(), table->store()->segments()[0]->size());
}

TEST(DynamicRollup, FormatIngestion)
{
  setenv("VIYA_TEST_ROLLUP_TS", "1496570140L", 1);

  db::Database db(std::move(util::Config(
        "{\"tables\": [{\"name\": \"events\","
        "               \"dimensions\": [{\"name\": \"install_time\","
        "                                 \"type\": \"time\","
        "                                 \"format\": \"%Y-%m-%d %T\","
        "                                 \"rollup_rules\": ["
        "                                   {\"granularity\": \"hour\",  \"after\": \"1 days\"},"
        "                                   {\"granularity\": \"day\",   \"after\": \"1 weeks\"},"
        "                                   {\"granularity\": \"month\", \"after\": \"1 years\"}"
        "                                ]}],"
        "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}")));

  auto table = db.GetTable("events");

  std::vector<query::MemoryRowOutput::Row> expected;

  // First day not rolled up:
  table->Load({
    {"2017-06-04 08:14:13"},
    {"2017-06-04 05:55:17"},
    {"2017-06-03 15:27:21"}
  });
  expected.push_back({"2017-06-04 08:14:13", "1"});
  expected.push_back({"2017-06-04 05:55:17", "1"});
  expected.push_back({"2017-06-03 15:27:21", "1"});

  // Rollup by hour after 1 day:
  table->Load({
    {"2017-06-03 08:14:13"},
    {"2017-06-02 18:13:22"},
    {"2017-06-02 18:24:10"}
  });
  expected.push_back({"2017-06-02 18:00:00", "2"});
  expected.push_back({"2017-06-03 08:00:00", "1"});

  // Rollup by day after 1 week:
  table->Load({
    {"2017-05-27 12:14:05"},
    {"2017-05-28 08:41:45"},
    {"2017-05-27 15:25:32"}
  });
  expected.push_back({"2017-05-27 00:00:00", "2"});
  expected.push_back({"2017-05-28 00:00:00", "1"});

  // Rollup by month after 1 year:
  table->Load({
    {"2016-02-03 18:21:09"},
    {"2012-06-03 12:31:57"},
    {"2016-02-04 04:44:45"}
  });
  expected.push_back({"2016-02-01 00:00:00", "2"});
  expected.push_back({"2012-06-01 00:00:00", "1"});

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"select\": [{\"column\": \"install_time\", \"format\": \"%Y-%m-%d %T\"},"
        "              {\"column\": \"count\"}],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"}}")), output);

  auto actual = output.rows();
  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
  EXPECT_EQ(1, table->store()->segments().size());
  EXPECT_EQ(expected.size(), table->store()->segments()[0]->size());
}

class TimeEvents : public testing::Test {
  protected:
    TimeEvents()
      :db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"install_time\","
              "                                 \"type\": \"time\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}"))) {}
    db::Database db;
};

TEST_F(TimeEvents, OutputFormat)
{
  auto table = db.GetTable("events");
  table->Load({
    {"US", "1415791501"},
    {"IL", "1447326002"}
  });

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"select\": [{\"column\": \"country\"},"
        "              {\"column\": \"install_time\", \"format\": \"%Y-%m-%d %T\"},"
        "              {\"column\": \"count\"}],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "2014-11-12 11:25:01", "1"},
    {"IL", "2015-11-12 11:00:02", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(TimeEvents, QueryGranularity)
{
  auto table = db.GetTable("events");
  table->Load({
    {"US", "1415791501"},
    {"IL", "1447326002"}
  });

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"select\": [{\"column\": \"country\"},"
        "              {\"column\": \"install_time\", \"format\": \"%Y-%m-%d %T\", \"granularity\": \"month\"},"
        "              {\"column\": \"count\"}],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "2014-11-01 00:00:00", "1"},
    {"IL", "2015-11-01 00:00:00", "1"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}
