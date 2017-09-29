#include <json.hpp>
#include "cluster/plan.h"
#include "util/config.h"
#include "gtest/gtest.h"

namespace cluster = viya::cluster;
namespace util = viya::util;

using json = nlohmann::json;

TEST(PlanGenerator, NoWorkers)
{
  util::Config cluster_config;
  std::vector<util::Config> worker_configs {};

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

  std::vector<util::Config> worker_configs {
    util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5000}"),
    util::Config("{\"hostname\": \"host2\", \"rack_id\": \"1\", \"http_port\": 5001}"),
    util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5002}")
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

  std::vector<util::Config> worker_configs {
    util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5000}"),
    util::Config("{\"hostname\": \"host2\", \"rack_id\": \"1\", \"http_port\": 5001}"),
    util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5002}")
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

  std::vector<util::Config> worker_configs {
    util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5000}"),
    util::Config("{\"hostname\": \"host1\", \"rack_id\": \"1\", \"http_port\": 5001}"),
    util::Config("{\"hostname\": \"host2\", \"rack_id\": \"2\", \"http_port\": 5000}"),
    util::Config("{\"hostname\": \"host2\", \"rack_id\": \"2\", \"http_port\": 5001}"),
    util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5000}"),
    util::Config("{\"hostname\": \"host3\", \"rack_id\": \"1\", \"http_port\": 5001}"),
    util::Config("{\"hostname\": \"host4\", \"rack_id\": \"2\", \"http_port\": 5000}"),
    util::Config("{\"hostname\": \"host4\", \"rack_id\": \"2\", \"http_port\": 5001}")
  };

  size_t partitions = 2;
  cluster::PlanGenerator plan_generator(cluster_config);
  auto actual = plan_generator.Generate(partitions, worker_configs);
  
  cluster::Plan expected(partitions);
  expected.AddPlacement(0, cluster::Placement("host1", 5000));
  expected.AddPlacement(0, cluster::Placement("host2", 5000));
  expected.AddPlacement(1, cluster::Placement("host3", 5000));
  expected.AddPlacement(1, cluster::Placement("host4", 5000));

  EXPECT_EQ(expected.ToJson(), actual.ToJson());
}
