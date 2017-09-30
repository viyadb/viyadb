#ifndef VIYA_CLUSTER_PLAN_H_
#define VIYA_CLUSTER_PLAN_H_

#include <string>
#include <vector>
#include <json.hpp>
#include "util/config.h"

namespace viya {
namespace cluster {

namespace util = viya::util;

using json = nlohmann::json;

class Placement {
  public:
    Placement(const std::string& hostname, uint16_t port):
      hostname_(hostname),port_(port) {}

    bool operator==(const Placement& other) const {
      return hostname_ == other.hostname_ && port_ == other.port_;
    }
    
    const std::string& hostname() const { return hostname_; }
    uint16_t port() const { return port_; }

  private:
    const std::string hostname_;
    uint16_t port_;
};

class Plan {
  public:
    Plan(size_t partitions_num):placements_(partitions_num, std::vector<Placement> {}) {}

    bool operator==(const Plan& other) const {
      return placements_ == other.placements_;
    }

    void AddPlacement(size_t partition, const Placement& placement) {
      placements_[partition].push_back(placement);
    }

    json ToJson() const;

  private:
    std::vector<std::vector<Placement>> placements_;
};

class PlanGenerator {
  public:
    PlanGenerator(const util::Config& cluster_config);
    PlanGenerator(const PlanGenerator& other) = delete;

    Plan Generate(size_t partitions_num, const std::vector<util::Config>& worker_configs);

  private:
    const util::Config& cluster_config_;
};

}}

#endif // VIYA_CLUSTER_PLAN_H_
