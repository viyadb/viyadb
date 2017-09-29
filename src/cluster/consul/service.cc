#include <stdexcept>
#include <json.hpp>
#include <cpr/cpr.h>
#include <glog/logging.h>
#include "cluster/consul/consul.h"
#include "util/hostname.h"

namespace viya {
namespace cluster {

Service::Service(const Consul& consul, const std::string& name, uint16_t port, uint32_t ttl_sec, bool auto_hc):
  consul_(consul),name_(name),port_(port),ttl_sec_(ttl_sec),id_(name + ":" + std::to_string(port)),repeat_(nullptr) {

  Register();

  if (auto_hc) {
    repeat_ = std::make_unique<util::Repeat>((ttl_sec * 2000L) / 3, [this]() {
      Notify(Status::OK, "Service is alive");
    });
  }
}

Service::~Service() {
  repeat_.reset();
  Deregister();
}

void Service::Deregister() {
  LOG(INFO)<<"Deregistering service '"<<id_<<"'";
  cpr::Put(
    cpr::Url { consul_.url() + "/v1/agent/service/deregister/" + id_ }
  );
}

void Service::Register() {
  json data = {
    { "id", id_ },
    { "name", name_ },
    { "address", util::get_hostname() },
    { "port", port_ },
    { "check", {{ "ttl", std::to_string(ttl_sec_) + "s" }} }
  };

  LOG(INFO)<<"Registering service '"<<id_<<"' with TTL="<<ttl_sec_<<"sec";;
  auto r = cpr::Put(
    cpr::Url { consul_.url() + "/v1/agent/service/register" },
    cpr::Body { data.dump() },
    cpr::Header {{ "Content-Type", "application/json" }}
  );
  if (r.status_code != 200) {
    if (r.status_code == 0) {
      throw std::runtime_error("Can't contact Consul (host is unreachable)");
    }
    throw std::runtime_error("Can't register new service (error: " + r.text + ")");
  }
}

void Service::Notify(Status status, const std::string& message) {
  std::string path;
  switch (status) {
    case Status::OK:   path = "pass"; break;
    case Status::FAIL: path = "fail"; break;
    case Status::WARN: path = "warn"; break;
  }
  DLOG(INFO)<<"Updating '"<<id_<<"' service status to '"<<path<<"'";
  auto r = cpr::Get(
    cpr::Url { consul_.url() + "/v1/agent/check/" + path + "/service:" + id_ },
    cpr::Parameters {{ "note", message }}
  );
  if (r.status_code == 404) {
    LOG(WARNING)<<"Service '"<<id_<<"' has been deregistered externally";
    Register();
  }
}

}}
