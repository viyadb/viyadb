#include <algorithm>
#include <gtest/gtest.h>
#include "db/table.h"
#include "util/config.h"
#include "query/output.h"
#include "db.h"
#include "input/simple.h"

namespace util = viya::util;
namespace query = viya::query;
namespace input = viya::input;

void sort_load_events(db::Table* table) {
  input::SimpleLoader loader(*table);
  loader.Load({
    {"US", "purchase", "20141110", "0.1"},
    {"IL", "refund", "20141111", "1.01"},
    {"CH", "refund", "20141111", "1.1"},
    {"AZ", "refund", "20141111", "1.1"},
    {"RU", "donate", "20141112", "1.0"},
    {"KZ", "review", "20141113", "5.0"}
  });
}

TEST_F(InappEvents, PaginateNoSort)
{
  auto table = db.GetTable("events");
  sort_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"country\", \"event_name\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"},"
        " \"skip\": 1,"
        " \"limit\": 2}")), output);

  EXPECT_EQ(2, output.rows().size());
}

TEST_F(InappEvents, SortMultiColumn)
{
  auto table = db.GetTable("events");
  sort_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"country\", \"event_name\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"},"
        " \"sort\": [{\"column\": \"revenue\", \"ascending\": false},"
        "            {\"column\": \"country\", \"ascending\": true}]}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"KZ", "review", "5"},
    {"AZ", "refund", "1.1"},
    {"CH", "refund", "1.1"},
    {"IL", "refund", "1.01"},
    {"RU", "donate", "1"},
    {"US", "purchase", "0.1"}
  };

  EXPECT_EQ(expected, output.rows());
}

TEST_F(InappEvents, TopN)
{
  auto table = db.GetTable("events");
  sort_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"},"
        " \"sort\": [{\"column\": \"revenue\"}, {\"column\": \"country\", \"ascending\": true}],"
        " \"limit\": 5}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"KZ", "5"},
    {"AZ", "1.1"},
    {"CH", "1.1"},
    {"IL", "1.01"},
    {"RU", "1"},
  };

  EXPECT_EQ(expected, output.rows());
}

TEST_F(InappEvents, LimitGreaterThanResult)
{
  auto table = db.GetTable("events");
  sort_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"},"
        " \"sort\": [{\"column\": \"revenue\"}, {\"column\": \"country\", \"ascending\": true}],"
        " \"limit\": 50}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"KZ", "5"},
    {"AZ", "1.1"},
    {"CH", "1.1"},
    {"IL", "1.01"},
    {"RU", "1"},
    {"US", "0.1"}
  };

  EXPECT_EQ(expected, output.rows());
}
