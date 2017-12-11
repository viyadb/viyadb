#include <algorithm>
#include <gtest/gtest.h>
#include "db/table.h"
#include "util/config.h"
#include "query/output.h"
#include "db.h"

namespace util = viya::util;
namespace query = viya::query;

TEST_F(MultiTenantEvents, PartitionBySingleColumn)
{
  LoadEvents(util::Config(
      "{\"partition_filter\": {"
      "  \"columns\": [\"app_id\"],"
      "  \"values\": [0],"
      "  \"total_partitions\": 5}}"
  ));

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"app_id\"],"
        " \"metrics\": [\"count\"]}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"com.horse", "3"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(MultiTenantEvents, PartitionByMultiColumn)
{
  LoadEvents(util::Config(
      "{\"partition_filter\": {"
      "  \"columns\": [\"app_id\", \"country\"],"
      "  \"values\": [0],"
      "  \"total_partitions\": 5}}"
  ));

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"app_id\", \"country\"],"
        " \"metrics\": [\"count\"]}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"com.bird", "KZ", "1"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

