#ifndef VIYA_TEST_DB_H_
#define VIYA_TEST_DB_H_

#include "gtest/gtest.h"
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

#endif // VIYA_TEST_DB_H_
