#ifndef VIYA_TEST_DB_H_
#define VIYA_TEST_DB_H_

#include "gtest/gtest.h"
#include "util/config.h"
#include "db/database.h"

namespace db = viya::db;
namespace util = viya::util;

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
    db::Database db;
};

#endif // VIYA_TEST_DB_H_
