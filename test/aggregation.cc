#include <algorithm>
#include "db/table.h"
#include "util/config.h"
#include "query/output.h"
#include "db.h"
#include "gtest/gtest.h"

namespace util = viya::util;
namespace query = viya::query;

void aggregation_load_events(db::Table* table) {
  table->Load({
    {"US", "purchase", "20141112", "0.1"},
    {"US", "purchase", "20141113", "1.1"},
    {"US", "donate", "20141112", "5.0"}
  });
}

void aggregation_load_num_events(db::Table* table) {
  table->Load({
    {"-1", "2", "-257", "145", "-10245", "23456", "-281734", "182745", "21.124", "1.092743"},
    {"-1", "7", "257", "14", "-10245", "1982", "0", "1111111111", "21.124", "1.092743"},
    {"-6", "2", "-257", "98", "-98", "777", "198", "0", "0.78", "7.912435"},
    {"9", "29", "-128", "145", "0", "23456", "-281734", "182745", "1.11", "139834.12313"}
  });
}

TEST_F(InappEvents, AggregationQuery)
{
  auto table = db.GetTable("events");
  aggregation_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"event_name\", \"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"eq\", \"column\": \"country\", \"value\": \"US\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"purchase", "US", "1.2"},
    {"donate", "US", "5"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, NoDimensions)
{
  auto table = db.GetTable("events");
  aggregation_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"revenue\", \"value\": \"0\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"6.2"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, OutputColumnsOrder)
{
  auto table = db.GetTable("events");
  aggregation_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"select\": [{\"column\": \"install_time\"}, {\"column\": \"country\"}, {\"column\": \"count\"}],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"20141112", "US", "2"},
    {"20141113", "US", "1"},
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, MetricFilter)
{
  auto table = db.GetTable("events");
  aggregation_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"event_name\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"revenue\", \"value\": \"4\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"donate", "5"}};
  EXPECT_EQ(expected, output.rows());
}

TEST_F(InappEvents, MissingValueEq)
{
  auto table = db.GetTable("events");
  aggregation_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"country\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"eq\", \"column\": \"country\", \"value\": \"RU\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {};
  EXPECT_EQ(expected, output.rows());
}

TEST_F(InappEvents, MissingValueNe)
{
  auto table = db.GetTable("events");
  aggregation_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"country\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"ne\", \"column\": \"country\", \"value\": \"RU\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "3"}
  };
  EXPECT_EQ(expected, output.rows());
}

TEST_F(NumericDimensions, AggregateQuery)
{
  auto table = db.GetTable("events");
  aggregation_load_num_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"byte\", \"float\", \"double\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"-1", "21.124", "1.092743", "2"},
    {"-6", "0.78", "7.912435", "1"},
    {"9", "1.11", "139834.12313", "1"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

