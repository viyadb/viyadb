#ifndef VIYA_COORD_CONTROLLER_H_
#define VIYA_COORD_CONTROLLER_H_

#include <memory>
#include "coord/consul/consul.h"
#include "util/config.h"

namespace viya {
namespace coord {

namespace util = viya::util;

class Controller {
  public:
    Controller(const util::Config& config);
    Controller(const Controller& other) = delete;

  private:
    const std::string cluster_id_;
    std::unique_ptr<Consul> consul_;
    std::unique_ptr<Session> session_;
    std::unique_ptr<LeaderElector> le_;
};

}}

#endif // VIYA_COORD_CONTROLLER_H_
