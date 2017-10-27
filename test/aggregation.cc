#include <algorithm>
#include <gtest/gtest.h>
#include "db/table.h"
#include "util/config.h"
#include "query/output.h"
#include "db.h"

namespace util = viya::util;
namespace query = viya::query;

TEST_F(InappEvents, AggregationQuery)
{
  LoadEvents();

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

TEST_F(InappEvents, HavingQuery)
{
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"event_name\", \"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"eq\", \"column\": \"country\", \"value\": \"US\"},"
        " \"having\": {\"op\": \"gt\", \"column\": \"revenue\", \"value\": \"2\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"donate", "US", "5"}
  };
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, ComplexHavingQuery)
{
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"event_name\", \"country\"],"
        " \"metrics\": [\"revenue\", \"count\"],"
        " \"filter\": {\"op\": \"eq\", \"column\": \"country\", \"value\": \"US\"},"
        " \"having\": {\"op\": \"and\", \"filters\": ["
        "              {\"op\": \"gt\", \"column\": \"revenue\", \"value\": \"1\"},"
        "              {\"op\": \"ge\", \"column\": \"count\", \"value\": \"2\"}]}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"purchase", "US", "1.2", "2"}
  };
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, NoDimensions)
{
  LoadEvents();

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
  LoadEvents();

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

TEST_F(InappEvents, ColumnsOrderHaving)
{
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"select\": [{\"column\": \"install_time\"}, {\"column\": \"country\"}, {\"column\": \"count\"}],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"},"
        " \"having\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"1\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"20141112", "US", "2"}
  };
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, MetricFilter)
{
  LoadEvents();

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

TEST_F(InappEvents, NoFilter)
{
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"event_name\"],"
        " \"metrics\": [\"revenue\"]}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"donate", "5"},
    {"purchase", "1.2"},
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, NoFilterHaving)
{
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"event_name\"],"
        " \"metrics\": [\"revenue\"],"
        " \"having\": {\"op\": \"gt\", \"column\": \"revenue\", \"value\": \"2\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"donate", "5"}
  };
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, MissingValueEq)
{
  LoadEvents();

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
  LoadEvents();

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
  LoadEvents();

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

