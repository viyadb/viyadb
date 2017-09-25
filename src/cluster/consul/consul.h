#ifndef VIYA_CLUSTER_CONSUL_H_
#define VIYA_CLUSTER_CONSUL_H_

#include <memory>
#include "util/config.h"
#include "cluster/consul/session.h"
#include "cluster/consul/service.h"
#include "cluster/consul/leader.h"
#include "cluster/consul/watch.h"

namespace viya {
namespace cluster {

namespace util = viya::util;

class Consul {
  public:
    Consul(const util::Config& config);
    Consul(const Consul& other) = delete;

    bool Enabled() const { return !url_.empty(); }

    std::unique_ptr<Session> CreateSession(const std::string& name, uint32_t ttl_sec = 10) const;
    std::unique_ptr<Service> RegisterService(const std::string& name, uint16_t port, uint32_t ttl_sec = 10, bool auto_hc = true) const;
    std::unique_ptr<LeaderElector> ElectLeader(const Session& session, const std::string& key) const;
    std::unique_ptr<Watch> WatchKey(const std::string& key) const;

    const std::string& url() const { return url_; }
    const std::string& prefix() const { return prefix_; }

  private:
    void CheckEnabled() const;

  private:
    std::string url_;
    std::string prefix_;
};

}}

#endif // VIYA_CLUSTER_CONSUL_H_
