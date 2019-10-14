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
#include "query/output.h"
#include "util/config.h"
#include <algorithm>
#include <gtest/gtest.h>

namespace util = viya::util;
namespace query = viya::query;

using AggregationEvents = InappEvents;

TEST_F(AggregationEvents, BasicQuery) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"event_name", "country"}},
          {"metrics", {"revenue"}},
          {"filter", {{"op", "eq"}, {"column", "country"}, {"value", "US"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"purchase", "US", "1.2"}, {"donate", "US", "5"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(AggregationEvents, QueryWithHeader) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"header", true},
          {"table", "events"},
          {"dimensions", {"country"}},
          {"metrics", {"revenue"}},
          {"filter", {{"op", "eq"}, {"column", "country"}, {"value", "US"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"country", "revenue"},
                                                       {"US", "6.2"}};
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(AggregationEvents, HavingQuery) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"event_name", "country"}},
          {"metrics", {"revenue"}},
          {"filter", {{"op", "eq"}, {"column", "country"}, {"value", "US"}}},
          {"having", {{"op", "gt"}, {"column", "revenue"}, {"value", "2"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"donate", "US", "5"}};
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(AggregationEvents, ComplexHavingQuery) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"event_name", "country"}},
          {"metrics", {"revenue", "count"}},
          {"filter", {{"op", "eq"}, {"column", "country"}, {"value", "US"}}},
          {"having",
           {{"op", "and"},
            {"filters",
             {{{"op", "gt"}, {"column", "revenue"}, {"value", "1"}},
              {{"op", "ge"}, {"column", "count"}, {"value", "2"}}}}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"purchase", "US", "1.2", "2"}};
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(AggregationEvents, NoDimensions) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", json::array()},
          {"metrics", {"revenue"}},
          {"filter", {{"op", "gt"}, {"column", "revenue"}, {"value", "0"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"6.2"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(AggregationEvents, OutputColumnsOrder) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"select",
           {{{"column", "install_time"}},
            {{"column", "country"}},
            {{"column", "count"}}}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"20141112", "US", "2"},
      {"20141113", "US", "1"},
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(AggregationEvents, ColumnsOrderHaving) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"select",
           {{{"column", "install_time"}},
            {{"column", "country"}},
            {{"column", "count"}}}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}},
          {"having", {{"op", "gt"}, {"column", "count"}, {"value", "1"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"20141112", "US", "2"}};
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(AggregationEvents, MetricFilter) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"event_name"}},
          {"metrics", {"revenue"}},
          {"filter", {{"op", "gt"}, {"column", "revenue"}, {"value", "4"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"donate", "5"}};
  EXPECT_EQ(expected, output.rows());
}

TEST_F(AggregationEvents, NoFilter) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(json{{"type", "aggregate"},
                                       {"table", "events"},
                                       {"dimensions", {"event_name"}},
                                       {"metrics", {"revenue"}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"donate", "5"},
      {"purchase", "1.2"},
  };
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(AggregationEvents, NoFilterHaving) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"event_name"}},
          {"metrics", {"revenue"}},
          {"having", {{"op", "gt"}, {"column", "revenue"}, {"value", "2"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"donate", "5"}};
  auto actual = output.rows();
  EXPECT_EQ(expected, actual);
}

TEST_F(AggregationEvents, HavingExtraColumn) {
  LoadEvents();

  query::MemoryRowOutput output;
  EXPECT_THROW(
      db.Query(std::move(util::Config(json{
                   {"type", "aggregate"},
                   {"table", "events"},
                   {"dimensions", {"event_name"}},
                   {"metrics", {"revenue"}},
                   {"having",
                    {{"op", "gt"}, {"column", "count"}, {"value", "1"}}}})),
               output),
      std::invalid_argument);
}

TEST_F(AggregationEvents, MissingValueEq) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"country"}},
          {"metrics", {"count"}},
          {"filter", {{"op", "eq"}, {"column", "country"}, {"value", "RU"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {};
  EXPECT_EQ(expected, output.rows());
}

TEST_F(AggregationEvents, MissingValueNe) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"country"}},
          {"metrics", {"count"}},
          {"filter", {{"op", "ne"}, {"column", "country"}, {"value", "RU"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {{"US", "3"}};
  EXPECT_EQ(expected, output.rows());
}

TEST_F(NumericDimensions, AggregateQuery) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(
      std::move(util::Config(json{
          {"type", "aggregate"},
          {"table", "events"},
          {"dimensions", {"byte", "float", "double"}},
          {"metrics", {"count"}},
          {"filter", {{"op", "gt"}, {"column", "count"}, {"value", "0"}}}})),
      output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"-1", "21.124", "1.092743", "2"},
      {"-6", "0.78", "7.912435", "1"},
      {"9", "1.11", "139834.12313", "1"}};
  std::sort(expected.begin(), expected.end());

  auto actual = output.rows();
  std::sort(actual.begin(), actual.end());

  EXPECT_EQ(expected, actual);
}

TEST_F(StringDimensions, AggregateQuery) {
  LoadEvents();

  query::MemoryRowOutput output;
  db.Query(std::move(util::Config(
               json{{"type", "aggregate"},
                    {"table", "events"},
                    {"dimensions", {"8bit", "16bit", "32bit", "64bit"}},
                    {"metrics", {"count"}}})),
           output);

  std::vector<query::MemoryRowOutput::Row> expected = {
      {"a", "b", "c", "d", "3"}};
  EXPECT_EQ(expected, output.rows());
}
