#ifndef VIYA_CLUSTER_LEADER_H_
#define VIYA_CLUSTER_LEADER_H_

#include <atomic>
#include <memory>
#include "cluster/consul/watch.h"
#include "util/schedule.h"

namespace viya {
namespace cluster {

class Consul;
class Session;

class LeaderElector {
  public:
    LeaderElector(const Consul& consul, const Session& session, const std::string& key);
    LeaderElector(const LeaderElector& other) = delete;

    bool Leader() const { return leader_; }

  private:
    void Start();

  private:
    const Session& session_;
    const std::string key_;
    std::unique_ptr<Watch> watch_;
    std::atomic<bool> leader_;
    std::unique_ptr<util::Always> always_;
};

}}

#endif // VIYA_CLUSTER_LEADER_H_
