#ifndef VIYA_CLUSTER_CONTROLLER_H_
#define VIYA_CLUSTER_CONTROLLER_H_

#include <memory>
#include <unordered_map>
#include "cluster/consul/consul.h"
#include "util/config.h"
#include "util/schedule.h"

namespace viya {
namespace cluster {

namespace util = viya::util;

class Controller {
  public:
    Controller(const util::Config& config);
    Controller(const Controller& other) = delete;

  private:
    void ReadClusterConfig();
    void GeneratePlan();

  private:
    const std::string cluster_id_;
    const consul::Consul consul_;
    util::Config cluster_config_;
    std::unique_ptr<consul::Session> session_;
    std::unique_ptr<consul::LeaderElector> le_;
    std::unique_ptr<util::Repeat> repeat_;
    std::unordered_map<std::string, util::Config> table_configs_;
    std::vector<std::string> cached_workers_;
};

}}

#endif // VIYA_CLUSTER_CONTROLLER_H_
