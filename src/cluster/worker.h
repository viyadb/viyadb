#ifndef VIYA_CLUSTER_WORKER_H_
#define VIYA_CLUSTER_WORKER_H_

#include <memory>
#include "cluster/consul/consul.h"
#include "util/config.h"

namespace viya {
namespace cluster {

namespace util = viya::util;

class Worker {
  public:
    Worker(const util::Config& config);
    Worker(const Worker& other) = delete;

    void CreateKey(const consul::Session& session) const;

  private:
    const util::Config& config_;
    const consul::Consul consul_;
    std::unique_ptr<consul::Session> session_;
};

}}

#endif // VIYA_CLUSTER_WORKER_H_
