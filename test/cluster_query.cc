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
#include <unordered_set>
#include <gtest/gtest.h>
#include "cluster/plan.h"
#include "cluster/partitioning.h"
#include "cluster/query.h"
#include "util/config.h"
#include "util/crc32.h"

namespace cluster = viya::cluster;
namespace util = viya::util;

class ClusterQuery : public testing::Test {
  protected:
    ClusterQuery():
      cluster_config("{\"replication_factor\": 1}"),
      worker_configs {
        {"worker1", util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5000}")},
        {"worker2", util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5001}")},
        {"worker3", util::Config("{\"hostname\": \"host2\", \"rack_id\": \"2\", \"http_port\": 5000}")},
        {"worker4", util::Config("{\"hostname\": \"host2\", \"rack_id\": \"2\", \"http_port\": 5001}")},
        {"worker5", util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5000}")},
        {"worker6", util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5001}")},
        {"worker7", util::Config("{\"hostname\": \"host4\", \"rack_id\": \"2\", \"http_port\": 5000}")},
        {"worker8", util::Config("{\"hostname\": \"host4\", \"rack_id\": \"2\", \"http_port\": 5001}")}
      },
      partitions_num(8),
      plan(cluster::PlanGenerator(cluster_config).Generate(partitions_num, worker_configs))
    {
    }

    uint32_t CalculateCode(const std::vector<std::string>& values) {
      uint32_t code = 0;
      for (auto& v : values) {
        code = crc32(code, v);
      }
      return code % partitions_num;
    }

    util::Config cluster_config;
    std::map<std::string, util::Config> worker_configs;
    size_t partitions_num;
    cluster::Plan plan;
};

TEST_F(ClusterQuery, SimpleCondition)
{
  std::vector<uint32_t> mapping(partitions_num);
  for (int i = 0; i < partitions_num; ++i) {
    mapping[i] = i;
  }
  cluster::Partitioning partitioning(mapping, partitions_num, std::vector<std::string> {"user"});

  std::string value("123456");
  util::Config query(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"user\", \"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"eq\", \"column\": \"user\", \"value\": \"" + value + "\"}}");

  cluster::ClusterQuery cluster_query(query, partitioning, plan);
  auto actual = cluster_query.target_workers();

  auto code = CalculateCode(std::vector<std::string> { value });
  auto workers = plan.partitions_workers()[partitioning.mapping()[code]];
  std::unordered_set<std::string> expected(workers.begin(), workers.end());

  EXPECT_EQ(actual.size(), 1);
  EXPECT_EQ(expected, actual);
}

TEST_F(ClusterQuery, MultipleColumns)
{
  std::vector<uint32_t> mapping(partitions_num);
  for (int i = 0; i < partitions_num; ++i) {
    mapping[i] = i;
  }
  cluster::Partitioning partitioning(mapping, partitions_num, std::vector<std::string> {"user", "date"});

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

  cluster::ClusterQuery cluster_query(query, partitioning, plan);
  auto actual = cluster_query.target_workers();

  std::unordered_set<std::string> expected;
  for (auto& v : std::vector<std::vector<std::string>> {{"a", "1"}, {"b", "2"}}) {
    auto code = CalculateCode(v);
    auto workers = plan.partitions_workers()[partitioning.mapping()[code]];
    expected.insert(workers.begin(), workers.end());
  }

  ASSERT_TRUE(actual.size() > 0);
  EXPECT_EQ(expected, actual);
}

TEST_F(ClusterQuery, ValuesCombinations)
{
  std::vector<uint32_t> mapping(partitions_num);
  for (int i = 0; i < partitions_num; ++i) {
    mapping[i] = i;
  }
  cluster::Partitioning partitioning(mapping, partitions_num, std::vector<std::string> {"user", "date"});

  util::Config query(
        "{\"type\": \"aggregate\","
        " \"table\": \"events\","
        " \"dimensions\": [\"user\", \"country\"],"
        " \"metrics\": [\"revenue\"],"
        " \"filter\": {\"op\": \"and\", \"filters\": ["
        "              {\"op\": \"in\", \"column\": \"user\", \"values\": [\"a\", \"b\"]},"
        "              {\"op\": \"in\", \"column\": \"date\", \"values\": [\"1\", \"2\"]}]}}");

  cluster::ClusterQuery cluster_query(query, partitioning, plan);
  auto actual = cluster_query.target_workers();

  std::unordered_set<std::string> expected;
  for (auto& v : std::vector<std::vector<std::string>> {{"a", "1"}, {"a", "2"}, {"b", "1"}, {"b", "2"}}) {
    auto code = CalculateCode(v);
    auto workers = plan.partitions_workers()[partitioning.mapping()[code]];
    expected.insert(workers.begin(), workers.end());
  }

  ASSERT_TRUE(actual.size() > 0);
  EXPECT_EQ(expected, actual);
}
