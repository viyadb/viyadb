#include <stdexcept>
#include <algorithm>
#include <glog/logging.h>
#include <cpr/cpr.h>
#include "coord/consul/consul.h"

namespace viya {
namespace coord {

Consul::Consul(const util::Config& config) {
  if (config.exists("consul")) {
    const auto& consul_conf = config.sub("consul");

    url_ = consul_conf.str("url");
    url_.erase(std::find_if(url_.rbegin(), url_.rend(), [](unsigned char ch) {
      return ch != '/';
    }).base(), url_.end());

    prefix_ = consul_conf.str("prefix", "viyadb");
  }
}

void Consul::CheckEnabled() const {
  if (!Enabled()) {
    throw std::runtime_error("Consul integration is disabled!");
  }
}

std::unique_ptr<Session> Consul::CreateSession(const std::string& name, uint32_t ttl_sec) const {
  CheckEnabled();
  return std::make_unique<Session>(*this, name, ttl_sec);
}

std::unique_ptr<Service> Consul::RegisterService(const std::string& name, uint16_t port, uint32_t ttl_sec, bool auto_hc) const {
  CheckEnabled();
  return std::make_unique<Service>(*this, name, port, ttl_sec, auto_hc);
}

std::unique_ptr<LeaderElector> Consul::ElectLeader(const Session& session, const std::string& key) const {
  CheckEnabled();
  return std::make_unique<LeaderElector>(*this, session, key);
}

std::unique_ptr<Watch> Consul::WatchKey(const std::string& key) const {
  CheckEnabled();
  return std::make_unique<Watch>(*this, key);
}

}}
