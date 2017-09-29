#include <algorithm>
#include "db/table.h"
#include "util/config.h"
#include "query/output.h"
#include "db.h"
#include "gtest/gtest.h"

namespace util = viya::util;
namespace query = viya::query;

TEST_F(LowCardColumn, Exceeded)
{
  auto table = db.GetTable("events");
  table->Load({
    {"GET"},
    {"POST"},
    {"HEAD"},
    {"PUT"},
    {"DELETE"},
    {"BLAH"},
    {"BLAH"}
  });

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"http_method\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"1\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"__exceeded", "2"}
  };
  EXPECT_EQ(expected, output.rows());
}

TEST_F(CardinalityGuard, Exceeded)
{
  auto table = db.GetTable("events");
  table->Load({
    {"13873844", "purchase"},
    {"13873844", "open-app"},
    {"13873844", "close-app"},
    {"13873844", "donate"},
    {"13873844", "click-add"},
    {"13873755", "purchase"},
    {"13873844", "purchase"}
  });

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"device_id\", \"event_name\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"13873844", "purchase", "2"},
    {"13873844", "open-app", "1"},
    {"13873844", "close-app", "1"},
    {"13873844", "__exceeded", "2"},
    {"13873755", "purchase", "1"},
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, DimensionLength)
{
  auto table = db.GetTable("events");
  table->Load({
    {"US", "veryveryveryveryveryveryveryverylongeventname", "20141112", "0.1"},
    {"US", "veryveryveryveryvery", "20141112", "0.1"}
  });

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"event_name\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"count\", \"value\": \"0\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"veryveryveryveryvery", "2"}
  };
  EXPECT_EQ(expected, output.rows());
}

