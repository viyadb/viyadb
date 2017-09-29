#include <algorithm>
#include "db/table.h"
#include "util/config.h"
#include "query/output.h"
#include "db.h"
#include "gtest/gtest.h"

namespace util = viya::util;
namespace query = viya::query;

void bitset_load_events(db::Table* table) {
  table->Load({
    {"US", "purchase", "1495475514", "12345"},
    {"RU", "support", "1495475517", "12346"},
    {"US", "openapp", "1495475632", "12347"},
    {"IL", "purchase", "1495475715", "12348"},
    {"KZ", "closeapp", "1495475716", "12349"},
    {"US", "uninstall", "1495475809", "12350"},
    {"KZ", "purchase", "1495475808", "12351"},
    {"US", "purchase", "1495476000", "12352"},
  });
}

TEST_F(UserEvents, BitsetMetric)
{
  auto table = db.GetTable("events");
  bitset_load_events(table);

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

