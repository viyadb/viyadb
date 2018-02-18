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

#include "cluster/plan.h"
#include "util/config.h"
#include <gtest/gtest.h>
#include <map>
#include <nlohmann/json.hpp>

namespace cluster = viya::cluster;
namespace util = viya::util;

using json = nlohmann::json;

TEST(PlanGenerator, NoWorkers) {
  std::map<std::string, util::Config> worker_configs{};

  cluster::PlanGenerator plan_generator;
  try {
    plan_generator.Generate(3, 3, worker_configs);
    FAIL();
  } catch (std::exception &e) {
    EXPECT_EQ("Can't place 3 copies of 3 partitions on 0 workers",
              std::string(e.what()));
  }
}

TEST(PlanGenerator, NotEnoughWorkers) {
  std::map<std::string, util::Config> worker_configs{
      {"host1:5000",
       json{{"hostname", "host1"}, {"rack_id", "1"}, {"http_port", 5000}}},
      {"host1:5001",
       json{{"hostname", "host2"}, {"rack_id", "1"}, {"http_port", 5001}}},
      {"host2:5000",
       json{{"hostname", "host3"}, {"rack_id", "1"}, {"http_port", 5002}}}};

  cluster::PlanGenerator plan_generator;
  try {
    plan_generator.Generate(3, 3, worker_configs);
    FAIL();
  } catch (std::exception &e) {
    EXPECT_EQ("Can't place 3 copies of 3 partitions on 3 workers",
              std::string(e.what()));
  }
}

TEST(PlanGenerator, LessReplicasThanRacks) {
  std::map<std::string, util::Config> worker_configs{
      {"host1:5000",
       json{{"hostname", "host1"}, {"rack_id", "1"}, {"http_port", 5000}}},
      {"host1:5001",
       json{{"hostname", "host2"}, {"rack_id", "1"}, {"http_port", 5001}}},
      {"host2:5000",
       json{{"hostname", "host3"}, {"rack_id", "1"}, {"http_port", 5002}}}};

  cluster::PlanGenerator plan_generator;
  try {
    plan_generator.Generate(1, 3, worker_configs);
    FAIL();
  } catch (std::exception &e) {
    EXPECT_EQ("Replication factor of 3 is smaller than the number of racks: 1",
              std::string(e.what()));
  }
}

TEST(PlanGenerator, Placement1) {
  std::map<std::string, util::Config> worker_configs{
      {"host1:5000",
       json{{"hostname", "host1"}, {"rack_id", "1"}, {"http_port", 5000}}},
      {"host1:5001",
       json{{"hostname", "host1"}, {"rack_id", "1"}, {"http_port", 5001}}},
      {"host2:5000",
       json{{"hostname", "host2"}, {"rack_id", "2"}, {"http_port", 5000}}},
      {"host2:5001",
       json{{"hostname", "host2"}, {"rack_id", "2"}, {"http_port", 5001}}},
      {"host3:5000",
       json{{"hostname", "host3"}, {"rack_id", "1"}, {"http_port", 5000}}},
      {"host3:5001",
       json{{"hostname", "host3"}, {"rack_id", "1"}, {"http_port", 5001}}},
      {"host4:5000",
       json{{"hostname", "host4"}, {"rack_id", "2"}, {"http_port", 5000}}},
      {"host4:5001",
       json{{"hostname", "host4"}, {"rack_id", "2"}, {"http_port", 5001}}}};

  size_t partitions_num = 2;
  cluster::PlanGenerator plan_generator;
  auto actual = plan_generator.Generate(partitions_num, 2, worker_configs);

  cluster::Partitions partitions(partitions_num, cluster::Replicas{});
  partitions[0].emplace_back("host1", 5000);
  partitions[0].emplace_back("host2", 5000);
  partitions[1].emplace_back("host3", 5000);
  partitions[1].emplace_back("host4", 5000);
  cluster::Plan expected(partitions);

  EXPECT_EQ(expected.ToJson(), actual.ToJson());
}
