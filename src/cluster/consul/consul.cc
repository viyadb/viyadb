#include <stdexcept>
#include <algorithm>
#include <glog/logging.h>
#include <cpr/cpr.h>
#include "cluster/consul/consul.h"

namespace viya {
namespace cluster {

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

std::unique_ptr<Session> Consul::CreateSession(const std::string& name,
    std::function<void(const Session&)> on_create, uint32_t ttl_sec) const {
  CheckEnabled();
  return std::make_unique<Session>(*this, name, on_create, ttl_sec);
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

std::vector<std::string> Consul::ListKeys(const std::string& key) const {
  auto r = cpr::Get(
    cpr::Url { url_ + "/v1/kv/" + prefix_ + "/" + key },
    cpr::Parameters {{ "keys", "true" }}
  );
  if (r.status_code == 0) {
    throw std::runtime_error("Can't contact Consul (host is unreachable)");
  }
  if (r.status_code == 404) {
    return std::move(std::vector<std::string> {});
  }
  if (r.status_code != 200) {
    throw std::runtime_error("Can't list keys (" + r.text + ")");
  }
  auto keys = json::parse(r.text).get<std::vector<std::string>>();
  std::for_each(keys.begin(), keys.end(), [](std::string& key) {
    key.erase(0, key.rfind("/") + 1);
  });
  return std::move(keys);
}

}}
