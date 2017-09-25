#ifndef VIYA_CLUSTER_CONTROLLER_H_
#define VIYA_CLUSTER_CONTROLLER_H_

#include <memory>
#include "cluster/consul/consul.h"
#include "util/config.h"

namespace viya {
namespace cluster {

namespace util = viya::util;

class Controller {
  public:
    Controller(const util::Config& config);
    Controller(const Controller& other) = delete;

  private:
    const std::string cluster_id_;
    const Consul consul_;
    std::unique_ptr<Session> session_;
    std::unique_ptr<LeaderElector> le_;
};

}}

#endif // VIYA_CLUSTER_CONTROLLER_H_
