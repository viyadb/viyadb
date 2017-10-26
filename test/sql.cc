#include <sstream>
#include <gtest/gtest.h>
#include "db/table.h"
#include "util/config.h"
#include "query/output.h"
#include "input/simple.h"
#include "sql/driver.h"
#include "db.h"

namespace util = viya::util;
namespace query = viya::query;
namespace input = viya::input;
namespace sql = viya::sql;

void sql_load_events(db::Table* table) {
  input::SimpleLoader loader(*table);
  loader.Load({
    {"US", "purchase", "20141112", "0.1"},
    {"US", "purchase", "20141113", "1.1"},
    {"US", "donate", "20141112", "5.0"}
  });
}

TEST_F(InappEvents, SqlSelect)
{
  auto table = db.GetTable("events");
  sql_load_events(table);

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT event_name,country,revenue FROM events WHERE country='US';");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"purchase", "US", "1.2"},
    {"donate", "US", "5"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, SqlSelectWildcard)
{
  auto table = db.GetTable("events");
  sql_load_events(table);

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT * FROM events WHERE country='US' AND revenue > 2;");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "donate", "20141112", "1", "5"}
  };

  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(InappEvents, SqlSelectNoFilter)
{
  auto table = db.GetTable("events");
  sql_load_events(table);

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT event_name FROM events");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"purchase"},
    {"donate"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

