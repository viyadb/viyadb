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
namespace sql = viya::sql;

using SqlEvents = InappEvents;
using SqlTimeEvents = TimeEvents;

TEST_F(SqlEvents, Select)
{
  LoadEvents();

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

TEST_F(SqlEvents, SelectWildcard)
{
  LoadEvents();

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

TEST_F(SqlEvents, SelectNoFilter)
{
  LoadEvents();

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

TEST_F(SqlEvents, SelectNotFilter)
{
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT event_name FROM events WHERE NOT event_name='donate'");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"purchase"}
  };
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectNotAndFilter)
{
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT * FROM events WHERE NOT (event_name='purchase' AND revenue > 1);");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"US", "donate", "20141112", "1", "5"},
    {"US", "purchase", "20141112", "1", "0.1"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectHaving)
{
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT event_name,count FROM events HAVING count <> 2");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"donate", "1"}
  };
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectSort)
{
  LoadSortEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT country,event_name,revenue FROM events ORDER BY revenue DESC, country");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"KZ", "review", "5"},
    {"AZ", "refund", "1.1"},
    {"CH", "refund", "1.1"},
    {"IL", "refund", "1.01"},
    {"RU", "donate", "1"},
    {"US", "purchase", "0.1"}
  };

  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, TopN)
{
  LoadSortEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT country,revenue FROM events ORDER BY revenue DESC, country LIMIT 5");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"KZ", "5"},
    {"AZ", "1.1"},
    {"CH", "1.1"},
    {"IL", "1.01"},
    {"RU", "1"},
  };

  EXPECT_EQ(expected, output.rows());
}

TEST_F(SqlEvents, SearchQuery)
{
  LoadSearchEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT SEARCH(event_name, 'r') FROM events WHERE revenue > 1.0 LIMIT 10");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"refund", "review"}
  };

  auto actual = output.rows();
  std::sort(actual[0].begin(), actual[0].end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SearchQueryLimit)
{
  LoadSearchEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT SEARCH(event_name, '') FROM events LIMIT 2");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"purchase", "refund"}
  };

  auto actual = output.rows();
  std::sort(actual[0].begin(), actual[0].end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectIn)
{
  LoadSortEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT country,event_name,revenue FROM events WHERE country IN ('CH', 'IL')");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"CH", "refund", "1.1"},
    {"IL", "refund", "1.01"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectBetween)
{
  LoadSortEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT install_time,event_name,count FROM events WHERE install_time BETWEEN 20141111 AND 20141112");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"20141111", "refund", "3"},
    {"20141112", "donate", "1"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlTimeEvents, SelectBetweenTimes)
{
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT install_time,event_name,count FROM events WHERE install_time BETWEEN '2015-01-03' AND '2015-01-04'");
  sql_driver.Run(query, output);

  std::vector<query::MemoryRowOutput::Row> expected = {
    {"1420257600", "closeapp", "1"},
    {"1420287194", "purchase", "1"}
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}
