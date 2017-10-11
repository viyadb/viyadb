#include <algorithm>
#include "db/table.h"
#include "db/database.h"
#include "util/config.h"
#include "query/output.h"
#include "gtest/gtest.h"

namespace db = viya::db;
namespace util = viya::util;
namespace query = viya::query;

class MetricEvents : public testing::Test {
  protected:
    MetricEvents()
      :db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"},"
              "                             {\"name\": \"byte_sum\", \"type\": \"byte_sum\"},"
              "                             {\"name\": \"byte_min\", \"type\": \"byte_min\"},"
              "                             {\"name\": \"byte_max\", \"type\": \"byte_max\"},"
              "                             {\"name\": \"byte_avg\", \"type\": \"byte_avg\"},"
              "                             {\"name\": \"ubyte_sum\", \"type\": \"ubyte_sum\"},"
              "                             {\"name\": \"ubyte_min\", \"type\": \"ubyte_min\"},"
              "                             {\"name\": \"ubyte_max\", \"type\": \"ubyte_max\"},"
              "                             {\"name\": \"ubyte_avg\", \"type\": \"ubyte_avg\"},"
              "                             {\"name\": \"short_sum\", \"type\": \"short_sum\"},"
              "                             {\"name\": \"short_min\", \"type\": \"short_min\"},"
              "                             {\"name\": \"short_max\", \"type\": \"short_max\"},"
              "                             {\"name\": \"short_avg\", \"type\": \"short_avg\"},"
              "                             {\"name\": \"ushort_sum\", \"type\": \"ushort_sum\"},"
              "                             {\"name\": \"ushort_min\", \"type\": \"ushort_min\"},"
              "                             {\"name\": \"ushort_max\", \"type\": \"ushort_max\"},"
              "                             {\"name\": \"ushort_avg\", \"type\": \"ushort_avg\"},"
              "                             {\"name\": \"int_sum\", \"type\": \"int_sum\"},"
              "                             {\"name\": \"int_min\", \"type\": \"int_min\"},"
              "                             {\"name\": \"int_max\", \"type\": \"int_max\"},"
              "                             {\"name\": \"int_avg\", \"type\": \"int_avg\"},"
              "                             {\"name\": \"uint_sum\", \"type\": \"uint_sum\"},"
              "                             {\"name\": \"uint_min\", \"type\": \"uint_min\"},"
              "                             {\"name\": \"uint_max\", \"type\": \"uint_max\"},"
              "                             {\"name\": \"uint_avg\", \"type\": \"uint_avg\"},"
              "                             {\"name\": \"long_sum\", \"type\": \"long_sum\"},"
              "                             {\"name\": \"long_min\", \"type\": \"long_min\"},"
              "                             {\"name\": \"long_max\", \"type\": \"long_max\"},"
              "                             {\"name\": \"long_avg\", \"type\": \"long_avg\"},"
              "                             {\"name\": \"ulong_sum\", \"type\": \"ulong_sum\"},"
              "                             {\"name\": \"ulong_min\", \"type\": \"ulong_min\"},"
              "                             {\"name\": \"ulong_max\", \"type\": \"ulong_max\"},"
              "                             {\"name\": \"ulong_avg\", \"type\": \"ulong_avg\"},"
              "                             {\"name\": \"float_sum\", \"type\": \"float_sum\"},"
              "                             {\"name\": \"float_min\", \"type\": \"float_min\"},"
              "                             {\"name\": \"float_max\", \"type\": \"float_max\"},"
              "                             {\"name\": \"float_avg\", \"type\": \"float_avg\"},"
              "                             {\"name\": \"double_sum\", \"type\": \"double_sum\"},"
              "                             {\"name\": \"double_min\", \"type\": \"double_min\"},"
              "                             {\"name\": \"double_max\", \"type\": \"double_max\"},"
              "                             {\"name\": \"double_avg\", \"type\": \"double_avg\"}]}]}"))) {}
    db::Database db;
};

void metrics_load_events(db::Table* table) {
  table->Load({
    {"US","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1"},
    {"US","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2"},
    {"US","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3","3"},
    {"IL","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1","1"},
    {"IL","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2","2"}
  });
}

TEST_F(MetricEvents, AggregateMetrics)
{
  auto table = db.GetTable("events");
  metrics_load_events(table);

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"country\"],"
        " \"metrics\": [\"count\","
        "               \"byte_sum\","
        "               \"byte_min\","
        "               \"byte_max\","
        "               \"byte_avg\","
        "               \"ubyte_sum\","
        "               \"ubyte_min\","
        "               \"ubyte_max\","
        "               \"ubyte_avg\","
        "               \"short_sum\","
        "               \"short_min\","
        "               \"short_max\","
        "               \"short_avg\","
        "               \"ushort_sum\","
        "               \"ushort_min\","
        "               \"ushort_max\","
        "               \"ushort_avg\","
        "               \"int_sum\","
        "               \"int_min\","
        "               \"int_max\","
        "               \"int_avg\","
        "               \"uint_sum\","
        "               \"uint_min\","
        "               \"uint_max\","
        "               \"uint_avg\","
        "               \"long_sum\","
        "               \"long_min\","
        "               \"long_max\","
        "               \"long_avg\","
        "               \"ulong_sum\","
        "               \"ulong_min\","
        "               \"ulong_max\","
        "               \"ulong_avg\","
        "               \"float_sum\","
        "               \"float_min\","
        "               \"float_max\","
        "               \"float_avg\","
        "               \"double_sum\","
        "               \"double_min\","
        "               \"double_max\","
        "               \"double_avg\"],"
        " \"filter\": {\"op\": \"ge\", \"column\": \"count\", \"value\": \"1\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US","3","6","1","3","2","6","1","3","2","6","1","3","2","6","1","3","2","6","1","3","2","6","1","3","2","6","1","3","2","6","1","3","2","6","1","3","2","6","1","3","2"},
    {"IL","2","3","1","2","1.5","3","1","2","1.5","3","1","2","1.5","3","1","2","1.5","3","1","2","1.5","3","1","2","1.5","3","1","2","1.5","3","1","2","1.5","3","1","2","1.5","3","1","2","1.5"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(MetricEvents, AverageWithoutCount)
{
  db::Database db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"}],"
              "               \"metrics\": [{\"name\": \"avg_revenue\", \"type\": \"double_avg\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "16"},
    {"US", "6"},
    {"US", "2.6"},
    {"IL", "20.7"},
    {"IL", "11"}
  });

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"country\"],"
        " \"metrics\": [\"avg_revenue\"],"
        " \"filter\": {\"op\": \"ge\", \"column\": \"avg_revenue\", \"value\": \"1\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "8.2"},
    {"IL", "15.85"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(MetricEvents, AverageQueryRollup)
{
  db::Database db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"}],"
              "               \"metrics\": [{\"name\": \"avg_revenue\", \"type\": \"double_avg\"}]}]}")));

  auto table = db.GetTable("events");
  table->Load({
    {"US", "16"},
    {"US", "6"},
    {"US", "2.6"},
    {"IL", "20.7"},
    {"IL", "11"}
  });

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [],"
        " \"metrics\": [\"avg_revenue\"],"
        " \"filter\": {\"op\": \"ge\", \"column\": \"avg_revenue\", \"value\": \"1\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"11.26"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

