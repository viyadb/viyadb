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
#include "db/store.h"
#include "db/table.h"
#include "input/simple.h"
#include <boost/filesystem.hpp>
#include <chrono>
#include <fstream>
#include <gtest/gtest.h>
#include <unistd.h>
#include <util/scope_guard.h>

namespace query = viya::query;
namespace input = viya::input;
namespace fs = boost::filesystem;

TEST_F(InappEvents, LoadEvents) {
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US", "purchase", "20141112", "0.1"},
               {"US", "purchase", "20141112", "1.1"}});

  EXPECT_EQ(1, table->store()->segments().size());
  EXPECT_EQ(1, table->store()->segments()[0]->size());
}

TEST_F(InappEvents, LoadMultipleSegments) {
  db::Database db(std::move(util::Config(
      json{{"tables",
            {{{"name", "events"},
              {"segment_size", 1},
              {"dimensions", {{{"name", "country"}}}},
              {"metrics", {{{"name", "count"}, {"type", "count"}}}}}}}})));
  auto table = db.GetTable("events");
  input::SimpleLoader loader(*table);
  loader.Load({{"US"}, {"IL"}, {"UK"}});

  EXPECT_EQ(3, table->store()->segments().size());
  EXPECT_EQ(1, table->store()->segments()[0]->size());
  EXPECT_EQ(1, table->store()->segments()[1]->size());
  EXPECT_EQ(1, table->store()->segments()[2]->size());
}

TEST_F(InappEvents, UpsertEvents) {
  auto table = db.GetTable("events");

  input::SimpleLoader loader1(*table);
  loader1.Load({{"US", "purchase", "20141112", "0.1"},
                {"US", "purchase", "20141112", "1.1"}});

  input::SimpleLoader loader2(*table);
  loader2.Load({{"US", "purchase", "20141112", "0.1"},
                {"US", "purchase", "20141112", "1.1"}});

  EXPECT_EQ(1, table->store()->segments().size());
  EXPECT_EQ(1, table->store()->segments()[0]->size());
}

TEST_F(InappEvents, LoadFromTsv) {
  auto table = db.GetTable("events");
  std::string fname("InappEvents_LoadFromTsv.tsv");
  std::ofstream out(fname);
  out << "US\tpurchase\t20141112\t0.1\n";
  out << "US\tpurchase\t20141112\t1.1\n";
  out << "US\t\t20141112\t0.3\n";
  out << "IL\tpurchase\t20141112\t0.0\n";
  out.close();

  util::Config load_conf(json{{"file", fname},
                              {"type", "file"},
                              {"format", "tsv"},
                              {"table", table->name()}});
  db.Load(load_conf);
  unlink(fname.c_str());

  EXPECT_EQ(1, table->store()->segments().size());
  EXPECT_EQ(3, table->store()->segments()[0]->size());

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"country"}},
          {"metrics", {"revenue"}},
          {"filter", {{"op", "ne"}, {"column", "country"}, {"value", "IL"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"US", "1.5"}};
  EXPECT_EQ(expected, output.rows());
}

TEST_F(InappEvents, LoadFromTsvCols) {
  auto table = db.GetTable("events");
  std::string fname("InappEvents_LoadFromTsvCols.tsv");
  std::ofstream out(fname);
  out << "purchase\tUS\t20141112\t1\t0.1\n";
  out << "purchase\tUS\t20141112\t2\t1.1\n";
  out << "\tUS\t20141112\t3\t0.3\n";
  out << "purchase\tIL\t20141112\t5\t0.0";
  out.close();

  util::Config load_conf(
      json{{"file", fname},
           {"type", "file"},
           {"columns",
            {"event_name", "country", "install_time", "index", "revenue"}},
           {"format", "tsv"},
           {"table", table->name()}});
  db.Load(load_conf);
  unlink(fname.c_str());

  EXPECT_EQ(1, table->store()->segments().size());
  EXPECT_EQ(3, table->store()->segments()[0]->size());

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"country"}},
          {"metrics", {"revenue"}},
          {"filter", {{"op", "ne"}, {"column", "country"}, {"value", "IL"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"US", "1.5"}};
  EXPECT_EQ(expected, output.rows());
}

TEST_F(InappEvents, LoadFromTsvColsMapping) {
  std::string fname("InappEvents_LoadFromTsvColsMapping.tsv");
  std::ofstream out(fname);
  out << "purchase\tUS\t20141112\t1\t0.1\n";
  out << "purchase\tUS\t20141112\t2\t1.1\n";
  out << "\tUS\t20141112\t3\t0.3\n";
  out << "purchase\tIL\t20141112\t5\t0.0\n";
  out.close();

  db::Database db(std::move(
      util::Config(json{{"tables",
                         {{{"name", "events"},
                           {"dimensions",
                            {{{"name", "country"}},
                             {{"name", "install_time"}, {"type", "time"}}}},
                           {"metrics",
                            {{{"name", "count"}, {"type", "count"}},
                             {{"name", "revenue_sum"},
                              {"type", "double_sum"},
                              {"field", "revenue"}},
                             {{"name", "revenue_max"},
                              {"type", "double_max"},
                              {"field", "revenue"}}}}}}}})));
  auto table = db.GetTable("events");

  util::Config load_conf(
      json{{"file", fname},
           {"type", "file"},
           {"columns",
            {"event_name", "country", "install_time", "index", "revenue"}},
           {"format", "tsv"},
           {"table", table->name()}});
  db.Load(load_conf);
  unlink(fname.c_str());

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"country"}},
          {"metrics", {"revenue_sum", "revenue_max"}},
          {"filter", {{"op", "ne"}, {"column", "country"}, {"value", "IL"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"US", "1.5", "1.1"}};
  EXPECT_EQ(expected, output.rows());
}

TEST(Watch, LoadEvents) {
  char tmpdir[] = "/tmp/viyadb-test-watch-load.XXXXXX";
  std::string load_dir(mkdtemp(tmpdir));
  util::ScopeGuard cleanup = [&load_dir]() { fs::remove_all(load_dir); };

  db::Database db(std::move(util::Config(json{
      {"tables",
       {{{"name", "events"},
         {"dimensions", {{{"name", "country"}}, {{"name", "event"}}}},
         {"metrics", {{{"name", "count"}, {"type", "count"}}}},
         {"watch", {{"directory", load_dir}, {"extensions", {".tsv"}}}}}}}})));
  auto table = db.GetTable("events");

  char tmpfile[] = "/tmp/viyadb-data.XXXXXX";
  mkstemp(tmpfile);
  std::ofstream out(tmpfile);
  out << "US\tpurchase\n";
  out << "US\tpurchase\n";
  out << "US\tsell\n";
  out << "IL\tpurchase\n";
  out.close();
  fs::rename(tmpfile, load_dir + "/input.tsv");

  for (int retries = 15; retries > 0 && table->store()->segments().empty();
       --retries) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{{"type", "aggregate"},
                                       {"table", "events"},
                                       {"dimensions", {"country", "event"}},
                                       {"metrics", {"count"}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"IL", "purchase", "1"}, {"US", "purchase", "2"}, {"US", "sell", "1"}};
  EXPECT_EQ(expected, output.rows());
}
