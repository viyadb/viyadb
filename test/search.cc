#include <algorithm>
#include <gtest/gtest.h>
#include "db/table.h"
#include "util/config.h"
#include "query/output.h"
#include "input/simple.h"
#include "db.h"

namespace util = viya::util;
namespace query = viya::query;

using SearchEvents = InappEvents;

TEST_F(SearchEvents, SearchQuery)
{
  LoadSearchEvents();

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

TEST_F(SearchEvents, SearchQueryLimit)
{
  LoadSearchEvents();

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
