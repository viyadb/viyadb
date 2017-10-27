#include <algorithm>
#include <gtest/gtest.h>
#include "db/table.h"
#include "util/config.h"
#include "query/output.h"
#include "input/simple.h"
#include "db.h"

namespace util = viya::util;
namespace query = viya::query;
namespace input = viya::input;

class SearchEvents: public InappEvents {
  protected:
    void LoadEvents() {
      auto table = db.GetTable("events");
      input::SimpleLoader loader(*table);
      loader.Load({
        {"US", "purchase", "20141110", "0.1"},
        {"IL", "refund", "20141111", "1.1"},
        {"CH", "refund", "20141111", "1.1"},
        {"AZ", "refund", "20141111", "1.1"},
        {"RU", "donate", "20141112", "1.0"},
        {"KZ", "review", "20141113", "5.0"}
      });
    }
};

TEST_F(SearchEvents, SearchQuery)
{
  LoadEvents();

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
  LoadEvents();

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
