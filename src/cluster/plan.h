#ifndef VIYA_CLUSTER_PLAN_H_
#define VIYA_CLUSTER_PLAN_H_

#include <vector>
#include <map>
#include <json.hpp>
#include "util/config.h"

namespace viya {
namespace cluster {

namespace util = viya::util;

using json = nlohmann::json;

class Placement {
  public:
    Placement(const std::string& hostname, uint16_t port):hostname_(hostname),port_(port) {}

    bool operator==(const Placement& other) const {
      return hostname_ == other.hostname_ && port_ == other.port_;
    }
    
    const std::string& hostname() const { return hostname_; }
    uint16_t port() const { return port_; }

  private:
    std::string hostname_;
    uint16_t port_;
};

using Replicas = std::vector<Placement>;
using Partitions = std::vector<Replicas>;

class Plan {
  public:
    Plan(const json& plan, const std::map<std::string, util::Config>& worker_configs);
    Plan(const Partitions& partitions, const std::map<std::string, util::Config>& worker_configs);
    Plan(const Plan& other) = delete;
    Plan(Plan&& other) = default;

    const Partitions& partitions() const { return partitions_; }
    const std::map<std::string, uint32_t>& workers_partitions() const { return workers_partitions_; }

    bool operator==(const Plan& other) const {
      return partitions_ == other.partitions_;
    }

    json ToJson() const;

  private:
    void AssignPartitionsToWorkers(const std::map<std::string, util::Config>& worker_configs);

  private:
    Partitions partitions_;
    std::map<std::string, uint32_t> workers_partitions_;
};

class PlanGenerator {
  public:
    PlanGenerator(const util::Config& cluster_config);
    PlanGenerator(const PlanGenerator& other) = delete;

    Plan Generate(size_t partitions_num,
                  const std::map<std::string, util::Config>& workers_configs);

  private:
    const util::Config& cluster_config_;
};

}}

#endif // VIYA_CLUSTER_PLAN_H_
