#include <algorithm>
#include "db/table.h"
#include "util/config.h"
#include "query/output.h"
#include "db.h"
#include "gtest/gtest.h"

namespace util = viya::util;
namespace query = viya::query;

void search_load_events(db::Table* table) {
  table->Load({
    {"US", "purchase", "20141110", "0.1"},
    {"IL", "refund", "20141111", "1.1"},
    {"CH", "refund", "20141111", "1.1"},
    {"AZ", "refund", "20141111", "1.1"},
    {"RU", "donate", "20141112", "1.0"},
    {"KZ", "review", "20141113", "5.0"}
  });
}

TEST_F(InappEvents, SearchQuery)
{
  auto table = db.GetTable("events");
  search_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"search\","
        " \"table\": \"events\","
        " \"dimension\": \"event_name\","
        " \"term\": \"r\","
        " \"limit\": 10,"
        " \"filter\": {\"op\": \"gt\", \"column\": \"revenue\", \"value\": \"1.0\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"refund", "review"}
  };

  auto actual = output.rows();
  std::sort(actual[0].begin(), actual[0].end());

  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, SearchQueryLimit)
{
  auto table = db.GetTable("events");
  search_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"search\","
        " \"table\": \"events\","
        " \"dimension\": \"event_name\","
        " \"term\": \"\","
        " \"limit\": 2,"
        " \"filter\": {\"op\": \"gt\", \"column\": \"install_time\", \"value\": \"0\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"purchase", "refund"}
  };

  auto actual = output.rows();
  std::sort(actual[0].begin(), actual[0].end());

  EXPECT_EQ(expected, actual);
}
