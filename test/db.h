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

#ifndef VIYA_TEST_DB_H_
#define VIYA_TEST_DB_H_

#include <gtest/gtest.h>
#include "util/config.h"
#include "db/database.h"
#include "input/simple.h"

namespace db = viya::db;
namespace util = viya::util;
namespace input = viya::input;

class InappEvents : public testing::Test {
  protected:
    InappEvents()
      :db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"event_name\", \"length\": 20},"
              "                                {\"name\": \"install_time\", \"type\": \"uint\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"},"
              "                             {\"name\": \"revenue\", \"type\": \"double_sum\"}]}]}"))) {}

    void LoadEvents() {
      auto table = db.GetTable("events");
      input::SimpleLoader loader(*table);
      loader.Load({
        {"US", "purchase", "20141112", "0.1"},
        {"US", "purchase", "20141113", "1.1"},
        {"US", "donate", "20141112", "5.0"}
      });
    }

    void LoadSortEvents() {
      auto table = db.GetTable("events");
      input::SimpleLoader loader(*table);
      loader.Load({
        {"US", "purchase", "20141110", "0.1"},
        {"IL", "refund", "20141111", "1.01"},
        {"CH", "refund", "20141111", "1.1"},
        {"AZ", "refund", "20141111", "1.1"},
        {"RU", "donate", "20141112", "1.0"},
        {"KZ", "review", "20141113", "5.0"}
      });
    }

    void LoadSearchEvents() {
      auto table = db.GetTable("events");
      input::SimpleLoader loader(*table);
      loader.Load({
        {"US", "purchase", "20141110", "0.1"},
        {"IL", "refund", "20141111", "1.1"},
        {"CH", "refund", "20141111", "1.1"},
        {"AZ", "refund", "20141111", "1.1"},
        {"RU", "donate", "20141112", "1.0"},
        {"KZ", "review", "20141113", "5.0"}
      });
    }

    db::Database db;
};

class NumericDimensions : public testing::Test {
  protected:
    NumericDimensions()
      :db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"byte\", \"type\": \"byte\"},"
              "                                {\"name\": \"ubyte\", \"type\": \"ubyte\"},"
              "                                {\"name\": \"short\", \"type\": \"short\"},"
              "                                {\"name\": \"ushort\", \"type\": \"ushort\"},"
              "                                {\"name\": \"int\", \"type\": \"int\"},"
              "                                {\"name\": \"uint\", \"type\": \"uint\"},"
              "                                {\"name\": \"long\", \"type\": \"long\"},"
              "                                {\"name\": \"ulong\", \"type\": \"ulong\"},"
              "                                {\"name\": \"float\", \"type\": \"float\"},"
              "                                {\"name\": \"double\", \"type\": \"double\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}"))) {}

    void LoadEvents() {
      auto table = db.GetTable("events");
      input::SimpleLoader loader(*table);
      loader.Load({
        {"-1", "2", "-257", "145", "-10245", "23456", "-281734", "182745", "21.124", "1.092743"},
        {"-1", "7", "257", "14", "-10245", "1982", "0", "1111111111", "21.124", "1.092743"},
        {"-6", "2", "-257", "98", "-98", "777", "198", "0", "0.78", "7.912435"},
        {"9", "29", "-128", "145", "0", "23456", "-281734", "182745", "1.11", "139834.12313"}
      });
    }

    db::Database db;
};

class UserEvents : public testing::Test {
  protected:
    UserEvents()
      :db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"event_name\"},"
              "                                {\"name\": \"time\", \"type\": \"uint\"}],"
              "               \"metrics\": [{\"name\": \"user_id\", \"type\": \"bitset\"}]}]}"))) {}

    void LoadEvents() {
      auto table = db.GetTable("events");
      input::SimpleLoader loader(*table);
      loader.Load({
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

    db::Database db;
};

class TimeEvents : public testing::Test {
  protected:
    TimeEvents()
      :db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"country\"},"
              "                                {\"name\": \"event_name\"},"
              "                                {\"name\": \"install_time\","
              "                                 \"type\": \"time\"}],"
              "               \"metrics\": [{\"name\": \"count\", \"type\": \"count\"}]}]}"))) {}

    void LoadEvents() {
      auto table = db.GetTable("events");
      input::SimpleLoader loader(*table);
      loader.Load({
        {"US", "purchase", "1420107084"},
        {"RU", "support", "1420150284"},
        {"US", "openapp", "1420178111"},
        {"IL", "purchase", "1420287194"},
        {"KZ", "closeapp", "1420257600"},
        {"US", "uninstall", "1420495805"},
        {"KZ", "purchase", "1420488000"},
        {"US", "purchase", "1420625132"},
      });
    }

    db::Database db;
};

class MultiTenantEvents : public testing::Test {
  protected:
    MultiTenantEvents()
      :db(std::move(util::Config(
              "{\"tables\": [{\"name\": \"events\","
              "               \"dimensions\": [{\"name\": \"app_id\"},"
              "                                {\"name\": \"country\"},"
              "                                {\"name\": \"event_name\"},"
              "                                {\"name\": \"time\", \"type\": \"uint\"}],"
              "               \"metrics\": [{\"name\": \"value\", \"type\": \"long_sum\"},"
              "                             {\"name\": \"count\", \"type\": \"count\"}]}]}"))) {}

    void LoadEvents(const util::Config& load_desc) {
      auto table = db.GetTable("events");
      input::SimpleLoader loader(load_desc, *table);
      loader.Load({
        {"com.bee", "US", "purchase", "1495475514", "15"},
        {"com.bee", "RU", "support", "1495475517", "23"},
        {"com.horse", "US", "openapp", "1495475632", "137"},
        {"com.horse", "IL", "purchase", "1495475715", "148"},
        {"com.horse", "KZ", "closeapp", "1495475716", "19"},
        {"com.bird", "US", "uninstall", "1495475809", "35"},
        {"com.bird", "KZ", "purchase", "1495475808", "231"},
        {"com.bird", "US", "purchase", "1495476000", "32"},
      });
    }

    db::Database db;
};

#endif // VIYA_TEST_DB_H_
