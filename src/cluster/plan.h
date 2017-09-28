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

class PlanGenerator {
  public:
    PlanGenerator(const util::Config& cluster_config);
    PlanGenerator(const PlanGenerator& other) = delete;

    json Generate(const json& partitions, const std::vector<util::Config>& worker_configs);

  private:
    const util::Config& cluster_config_;
};

}}

#endif // VIYA_CLUSTER_PLAN_H_
