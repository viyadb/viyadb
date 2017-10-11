#include <algorithm>
#include "db/table.h"
#include "db/database.h"
#include "util/config.h"
#include "query/output.h"
#include "gtest/gtest.h"

namespace db = viya::db;
namespace util = viya::util;
namespace query = viya::query;

class LiteEvents : public testing::Test {
  protected:
    LiteEvents()
      :db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"time\", \"type\": \"ulong\", \"max\": 4000000},"
              "                                {\"name\": \"dummy\", \"max\": 1}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}"))) {}
    db::Database db;
};

TEST_F(LiteEvents, SegmentsSkipped)
{
  auto table = db.GetTable("events");
  std::vector<std::string> row(2);
  row[1] = "";
  unsigned int start_time = 1996111U;
  unsigned int end_time = start_time + table->segment_size() + std::min(table->segment_size(), 100UL);
  for (unsigned int time = start_time; time < end_time; ++time) {
    row[0] = std::to_string(time);
    table->Load(row);
  }

  unsigned int segment2_start = start_time + table->segment_size();
  query::MemoryRowOutput output;
  auto stats = db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"time\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"and\", \"filters\": ["
        "               {\"op\": \"gt\", \"column\": \"time\", \"value\": \"" + std::to_string(segment2_start) + "\"},"
        "               {\"op\": \"ne\", \"column\": \"dummy\", \"value\": \"bla\"}"
        "             ]}}")), output);

  EXPECT_EQ(1, stats.scanned_segments);
}
