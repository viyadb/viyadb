#include <gtest/gtest.h>
#include "util/config.h"
#include "db/table.h"
#include "db/database.h"
#include "query/output.h"
#include "input/simple.h"

namespace db = viya::db;
namespace util = viya::util;
namespace query = viya::query;
namespace input = viya::input;

class BoolDimEvents : public testing::Test {
  protected:
    BoolDimEvents()
      :db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"event_name\"},"
              "                                {\"name\": \"is_organic\", \"type\": \"boolean\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}"))) {}
    db::Database db;
};

TEST_F(BoolDimEvents, QueryTest)
{
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({
    {"purchase", "true"},
    {"purchase", "false"},
    {"open", "true"},
    {"open", "false"},
    {"open", "true"}
  });

  query::MemoryRowOutput output;
  db.Query(
    std::move(util::Config(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"event_name\", \"is_organic\"],"
        " \"metrics\": [\"count\"],"
        " \"filter\": {\"op\": \"eq\", \"column\": \"is_organic\", \"value\": \"true\"}}")), output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"purchase", "true", "1"},
    {"open", "true", "2"}
  };
  auto actual = output.rows();

  std::sort(expected.begin(), expected.end());
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

