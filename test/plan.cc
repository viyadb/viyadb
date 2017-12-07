#include <json.hpp>
#include <map>
#include "cluster/plan.h"
#include "util/config.h"
#include "gtest/gtest.h"

namespace cluster = viya::cluster;
namespace util = viya::util;

using json = nlohmann::json;

TEST(PlanGenerator, NoWorkers)
{
  util::Config cluster_config;
  std::map<std::string, util::Config> worker_configs {};

  cluster::PlanGenerator plan_generator(cluster_config);
  try {
    plan_generator.Generate(3, worker_configs);
    FAIL();
  } catch (std::exception& e) {
    EXPECT_EQ("Can't place 3 copies of 3 partitions on 0 workers", std::string(e.what()));
  }
}

TEST(PlanGenerator, NotEnoughWorkers)
{
  util::Config cluster_config(
    "{\"replication_factor\": 3}"
  );

  std::map<std::string, util::Config> worker_configs {
    {"worker1", util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5000}")},
    {"worker2", util::Config("{\"hostname\": \"host2\", \"rack_id\": \"1\", \"http_port\": 5001}")},
    {"worker3", util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5002}")}
  };

  cluster::PlanGenerator plan_generator(cluster_config);
  try {
    plan_generator.Generate(3, worker_configs);
    FAIL();
  } catch (std::exception& e) {
    EXPECT_EQ("Can't place 3 copies of 3 partitions on 3 workers", std::string(e.what()));
  }
}

TEST(PlanGenerator, LessReplicasThanRacks)
{
  util::Config cluster_config(
    "{\"replication_factor\": 3}"
  );

  std::map<std::string, util::Config> worker_configs {
    {"worker1", util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5000}")},
    {"worker2", util::Config("{\"hostname\": \"host2\", \"rack_id\": \"1\", \"http_port\": 5001}")},
    {"worker3", util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5002}")}
  };

  cluster::PlanGenerator plan_generator(cluster_config);
  try {
    plan_generator.Generate(1, worker_configs);
    FAIL();
  } catch (std::exception& e) {
    EXPECT_EQ("Replication factor of 3 is smaller than the number of racks: 1", std::string(e.what()));
  }
}

TEST(PlanGenerator, Placement1)
{
  util::Config cluster_config(
    "{\"replication_factor\": 2}"
  );

  std::map<std::string, util::Config> worker_configs {
    {"worker1", util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5000}")},
    {"worker2", util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5001}")},
    {"worker3", util::Config("{\"hostname\": \"host2\", \"rack_id\": \"2\", \"http_port\": 5000}")},
    {"worker4", util::Config("{\"hostname\": \"host2\", \"rack_id\": \"2\", \"http_port\": 5001}")},
    {"worker5", util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5000}")},
    {"worker6", util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5001}")},
    {"worker7", util::Config("{\"hostname\": \"host4\", \"rack_id\": \"2\", \"http_port\": 5000}")},
    {"worker8", util::Config("{\"hostname\": \"host4\", \"rack_id\": \"2\", \"http_port\": 5001}")}
  };

  size_t partitions_num = 2;
  cluster::PlanGenerator plan_generator(cluster_config);
  auto actual = plan_generator.Generate(partitions_num, worker_configs);
  
  cluster::Partitions partitions(partitions_num, cluster::Replicas {});
  partitions[0].emplace_back("host1", 5000);
  partitions[0].emplace_back("host2", 5000);
  partitions[1].emplace_back("host3", 5000);
  partitions[1].emplace_back("host4", 5000);
  cluster::Plan expected(partitions, worker_configs);

  EXPECT_EQ(expected.ToJson(), actual.ToJson());
}
