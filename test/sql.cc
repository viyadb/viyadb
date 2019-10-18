/*
 * Copyright (c) 2017 ViyaDB Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "db.h"
#include "db/table.h"
#include "input/simple.h"
#include "query/output.h"
#include "sql/driver.h"
#include "util/config.h"
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>

namespace util = viya::util;
namespace query = viya::query;
namespace sql = viya::sql;

using SqlEvents = InappEvents;
using SqlTimeEvents = TimeEvents;

TEST_F(SqlEvents, ErrorUnknownTable) {
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query(
      "SELECT event_name,country,revenue FROM unknown WHERE country='US';");
  try {
    sql_driver.Run(query, &output);
    FAIL() << "Should throw exception on unknown table!";
  } catch (std::exception &e) {
    EXPECT_EQ(std::string("No such table: unknown"), e.what());
  }
}

TEST_F(SqlEvents, ErrorSyntax) {
  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query(
      "SELECT event_name, FROM events WHERE country='US';");
  try {
    sql_driver.Run(query, &output);
    FAIL() << "Should throw exception on syntax error!";
  } catch (std::exception &e) {
    EXPECT_EQ(0, strncmp(e.what(), "syntax error", strlen("syntax error")));
  }
}

TEST_F(SqlEvents, Select) {
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query(
      "SELECT event_name,country,revenue FROM events WHERE country='US';");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"purchase", "US", "1.2"}, {"donate", "US", "5"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectWildcard) {
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query(
      "SELECT * FROM events WHERE country='US' AND revenue > 2;");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "donate", "20141112", "1", "5"}};

  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectNoFilter) {
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT event_name FROM events");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"purchase"},
                                                       {"donate"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectNotFilter) {
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query(
      "SELECT event_name FROM events WHERE NOT event_name='donate'");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"purchase"}};
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectNotAndFilter) {
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT * FROM events WHERE NOT "
                           "(event_name='purchase' AND revenue > 1);");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "donate", "20141112", "1", "5"},
      {"US", "purchase", "20141112", "1", "0.1"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectHaving) {
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query(
      "SELECT event_name,count FROM events HAVING count <> 2");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"donate", "1"}};
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectSort) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT country,event_name,revenue FROM events "
                           "ORDER BY revenue DESC, country");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"KZ", "review", "5"},   {"AZ", "refund", "1.1"},
      {"CH", "refund", "1.1"}, {"IL", "refund", "1.01"},
      {"RU", "donate", "1"},   {"US", "purchase", "0.1"}};

  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, TopN) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT country,revenue FROM events ORDER BY "
                           "revenue DESC, country LIMIT 5");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"KZ", "5"}, {"AZ", "1.1"}, {"CH", "1.1"}, {"IL", "1.01"}, {"RU", "1"}};

  EXPECT_EQ(expected, output.rows());
}

TEST_F(SqlEvents, LimitOffset) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT country,revenue FROM events ORDER BY "
                           "revenue DESC, country LIMIT 2, 3");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"CH", "1.1"}, {"IL", "1.01"}, {"RU", "1"}};

  EXPECT_EQ(expected, output.rows());
}

TEST_F(SqlEvents, LimitOffset2) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT country,revenue FROM events ORDER BY "
                           "revenue DESC, country LIMIT 3 OFFSET 2");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"CH", "1.1"}, {"IL", "1.01"}, {"RU", "1"}};

  EXPECT_EQ(expected, output.rows());
}

TEST_F(SqlEvents, SelectRawAll) {
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT RAW(*) FROM events");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "purchase", "20141112", "1", "0.1"},
      {"US", "purchase", "20141113", "1", "1.1"},
      {"US", "donate", "20141112", "1", "5"}};
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectRawSlice) {
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT RAW(country, event_name) FROM events");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"US", "purchase"}, {"US", "purchase"}, {"US", "donate"}};
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectRawLimit) {
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT RAW(country) FROM events LIMIT 1");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"US"}};
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SearchQuery) {
  LoadSearchEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT SEARCH(event_name, 'r') FROM events WHERE "
                           "revenue > 1.0 LIMIT 10");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"refund", "review"}};

  auto actual = output.rows();
  std::sort(actual[0].begin(), actual[0].end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SearchQueryLimit) {
  LoadSearchEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT SEARCH(event_name, '') FROM events LIMIT 2");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"purchase", "refund"}};

  auto actual = output.rows();
  std::sort(actual[0].begin(), actual[0].end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectIn) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT country,event_name,revenue FROM events "
                           "WHERE country IN ('CH', 'IL')");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"CH", "refund", "1.1"}, {"IL", "refund", "1.01"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SelectBetween) {
  LoadSortEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT install_time,event_name,count FROM events "
                           "WHERE install_time BETWEEN 20141111 AND 20141112");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"20141111", "refund", "3"}, {"20141112", "donate", "1"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlTimeEvents, SelectBetweenTimes) {
  LoadEvents();

  query::MemoryRowOutput output;
  sql::Driver sql_driver(db);

  std::istringstream query("SELECT install_time,event_name,count FROM events "
                           "WHERE install_time BETWEEN '2015-01-03' AND "
                           "'2015-01-04'");
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"1420257600", "closeapp", "1"}, {"1420287194", "purchase", "1"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, CopyFromFile) {
  std::string fname("SqlEvents_CopyFromFile.tsv");
  std::ofstream out(fname);
  out << "purchase\tUS\t20141112\t0.1\n";
  out << "purchase\tUS\t20141112\t1.1\n";
  out << "\tUS\t20141112\t0.3\n";
  out << "purchase\tIL\t20141112\t0.0\n";
  out.close();

  sql::Driver sql_driver(db);

  std::istringstream copy_stmt(
      "COPY events (event_name,country,install_time,revenue) FROM '" + fname +
      "' FORMAT TSV");
  sql_driver.Run(copy_stmt);
  unlink(fname.c_str());

  query::MemoryRowOutput output;
  std::istringstream query("SELECT country,count FROM events");
  sql_driver.Reset();
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"US", "3"},
                                                       {"IL", "1"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(SqlEvents, SupportHypens) {
  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "user-events"},
              {"dimensions", {{{"name", "app-id"}}, {{"name", "event-name"}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));

  auto table = db.GetTable("user-events");
  input::SimpleLoader loader(*table);
  loader.Load({{"a.b.c", "purchase"},
               {"a.b.c", "sell"},
               {"x.y.z", "purchase"},
               {"a.b.c", "sell"},
               {"x.y.z", "purchase"}});

  sql::Driver sql_driver(db);

  query::MemoryRowOutput output;
  std::istringstream query("SELECT `app-id`,count FROM \"user-events\"");
  sql_driver.Reset();
  sql_driver.Run(query, &output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"a.b.c", "3"},
                                                       {"x.y.z", "2"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}
