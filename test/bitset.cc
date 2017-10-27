#include <algorithm>
#include <gtest/gtest.h>
#include "db/table.h"
#include "db/database.h"
#include "util/config.h"
#include "query/output.h"
#include "db.h"

namespace util = viya::util;
namespace query = viya::query;

TEST_F(UserEvents, BitsetMetric)
{
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"country\"],"
        " \"metrics\": [\"user_id\"],"
        " \"filter\": {\"op\": \"gt\", \"column\": \"time\", \"value\": \"1495475514\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"RU", "1"},
    {"IL", "1"},
    {"US", "3"},
    {"KZ", "2"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

