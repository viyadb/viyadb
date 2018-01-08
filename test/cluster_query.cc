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

#include <map>
#include <gtest/gtest.h>
#include <random>
#include "cluster/plan.h"
#include "cluster/partitioning.h"
#include "cluster/query/query.h"
#include "util/config.h"
#include "util/crc32.h"

namespace cluster = viya::cluster;
namespace util = viya::util;

class ClusterQuery : public testing::Test {
  protected:
    ClusterQuery(int replicas = 1, int partitions = 8):
      cluster_config("{\"replication_factor\": " + std::to_string(replicas) + "}"),
      worker_configs {
        {"host1:5000", util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5000}")},
        {"host1:5001", util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5001}")},
        {"host2:5000", util::Config("{\"hostname\": \"host2\", \"rack_id\": \"2\", \"http_port\": 5000}")},
        {"host2:5001", util::Config("{\"hostname\": \"host2\", \"rack_id\": \"2\", \"http_port\": 5001}")},
        {"host3:5000", util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5000}")},
        {"host3:5001", util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5001}")},
        {"host4:5000", util::Config("{\"hostname\": \"host4\", \"rack_id\": \"2\", \"http_port\": 5000}")},
        {"host4:5001", util::Config("{\"hostname\": \"host4\", \"rack_id\": \"2\", \"http_port\": 5001}")}
      },
      partitions_num(partitions),
      partition_mapping(partitions),
      plan(cluster::PlanGenerator(cluster_config).Generate(partitions_num, worker_configs))
    {
      for (int i = 0; i < partitions_num; ++i) {
        partition_mapping[i] = i;
      }
    }

    uint32_t CalculateCode(const std::vector<std::string>& values) {
      uint32_t code = 0;
      for (auto& v : values) {
        code = crc32(code, v);
      }
      return code % partitions_num;
    }

    std::vector<uint32_t> partition_mapping;
    util::Config cluster_config;
    std::map<std::string, util::Config> worker_configs;
    size_t partitions_num;
    cluster::Plan plan;
};

class ClusterQuery2Replicas : public ClusterQuery {
  protected:
    ClusterQuery2Replicas():ClusterQuery(2, 4) {}
};

TEST_F(ClusterQuery, SimpleCondition)
{
  cluster::Partitioning partitioning(
    partition_mapping, partitions_num, std::vector<std::string> {"user"});

  std::string value("123456");
  util::Config query(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"user\", \"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"eq\", \"column\": \"user\", \"value\": \"" + value + "\"}}");

  cluster::query::ClusterQuery cluster_query(query, partitioning, plan);
  auto actual = cluster_query.target_workers();

  auto code = CalculateCode(std::vector<std::string> { value });
  auto& expected = plan.partitions_workers()[partitioning.mapping()[code]];

  EXPECT_EQ(actual.size(), 1);
  EXPECT_EQ(actual[0].size(), 1);
  EXPECT_EQ(expected, actual[0]);
}

TEST_F(ClusterQuery, NonKeyFieldCondition)
{
  cluster::Partitioning partitioning(
    partition_mapping, partitions_num, std::vector<std::string> {"user"});

  std::string value("123456");
  util::Config query(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"user\", \"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"and\", \"filters\": ["
        "               {\"op\": \"eq\", \"column\": \"user\", \"value\": \"" + value + "\"},"
        "               {\"op\": \"ne\", \"column\": \"country\", \"value\": \"\"}]}}");

  cluster::query::ClusterQuery cluster_query(query, partitioning, plan);
  auto actual = cluster_query.target_workers();

  auto code = CalculateCode(std::vector<std::string> { value });
  auto& expected = plan.partitions_workers()[partitioning.mapping()[code]];

  EXPECT_EQ(actual.size(), 1);
  EXPECT_EQ(actual[0].size(), 1);
  EXPECT_EQ(expected, actual[0]);
}

TEST_F(ClusterQuery, MultipleColumns)
{
  cluster::Partitioning partitioning(
    partition_mapping, partitions_num, std::vector<std::string> {"user", "date"});

  util::Config query(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"user\", \"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"or\", \"filters\": ["
        "              {\"op\": \"and\", \"filters\": ["
        "               {\"op\": \"eq\", \"column\": \"user\", \"value\": \"a\"},"
        "               {\"op\": \"eq\", \"column\": \"date\", \"value\": \"1\"}]},"
        "              {\"op\": \"and\", \"filters\": ["
        "               {\"op\": \"eq\", \"column\": \"user\", \"value\": \"b\"},"
        "               {\"op\": \"eq\", \"column\": \"date\", \"value\": \"2\"}]}]}}");

  cluster::query::ClusterQuery cluster_query(query, partitioning, plan);
  auto actual = cluster_query.target_workers();

  std::vector<std::vector<std::string>> expected {{"host1:5001"}, {"host3:5001"}};
  EXPECT_EQ(expected, actual);
}

TEST_F(ClusterQuery, ValuesCombinations)
{
  cluster::Partitioning partitioning(
    partition_mapping, partitions_num, std::vector<std::string> {"user", "date"});

  util::Config query(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"user\", \"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"and\", \"filters\": ["
        "              {\"op\": \"in\", \"column\": \"user\", \"values\": [\"a\", \"b\"]},"
        "              {\"op\": \"in\", \"column\": \"date\", \"values\": [\"1\", \"2\"]}]}}");

  cluster::query::ClusterQuery cluster_query(query, partitioning, plan);
  auto actual = cluster_query.target_workers();

  std::vector<std::vector<std::string>> expected {{"host1:5001"}, {"host1:5000"}, {"host3:5001"}, {"host3:5000"}};
  EXPECT_EQ(expected, actual);
}

TEST_F(ClusterQuery2Replicas, SimpleCondition)
{
  cluster::Partitioning partitioning(
    partition_mapping, partitions_num, std::vector<std::string> {"user"});

  std::string value("123456");
  util::Config query(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"user\", \"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"eq\", \"column\": \"user\", \"value\": \"" + value + "\"}}");

  cluster::query::ClusterQuery cluster_query(query, partitioning, plan);
  auto actual = cluster_query.target_workers();

  auto code = CalculateCode(std::vector<std::string> { value });
  auto& expected = plan.partitions_workers()[partitioning.mapping()[code]];

  EXPECT_EQ(actual.size(), 1);
  EXPECT_EQ(expected, actual[0]);
}

TEST_F(ClusterQuery2Replicas, MultipleColumns)
{
  std::srand(std::time(0));
  cluster::Partitioning partitioning(
    partition_mapping, partitions_num, std::vector<std::string> {"user", "date"});

  util::Config query(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"user\", \"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"or\", \"filters\": ["
        "              {\"op\": \"and\", \"filters\": ["
        "               {\"op\": \"eq\", \"column\": \"user\", \"value\": \"a\"},"
        "               {\"op\": \"eq\", \"column\": \"date\", \"value\": \"1\"}]},"
        "              {\"op\": \"and\", \"filters\": ["
        "               {\"op\": \"eq\", \"column\": \"user\", \"value\": \"b\"},"
        "               {\"op\": \"eq\", \"column\": \"date\", \"value\": \"2\"}]}]}}");

  cluster::query::ClusterQuery cluster_query(query, partitioning, plan);
  auto actual = cluster_query.target_workers();

  std::vector<std::vector<std::string>> expected {
    {"host1:5001", "host2:5001"},
    {"host3:5001", "host4:5001"}
  };
  EXPECT_EQ(expected, actual);
}
