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

using PartitionPlacements = std::vector<Placement>;
using TablePlacements = std::vector<PartitionPlacements>;

class Plan {
  public:
    Plan(const json& plan);
    Plan(const TablePlacements& placements);
    Plan(const Plan& other) = delete;
    Plan(Plan&& other) = default;

    const TablePlacements& placements() const { return placements_; }

    bool operator==(const Plan& other) const {
      return placements_ == other.placements_;
    }

    json ToJson() const;

  private:
    TablePlacements placements_;
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
