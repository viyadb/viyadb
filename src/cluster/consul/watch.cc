#include <cpr/cpr.h>
#include <glog/logging.h>
#include "cluster/consul/consul.h"
#include "cluster/consul/watch.h"

namespace viya {
namespace cluster {
namespace consul {

Watch::Watch(const Consul& consul, const std::string& key)
  :consul_(consul),key_(consul.prefix() + "/" + key)
   ,url_(consul_.url() + "/v1/kv/" + key_),index_(1) {
}

json Watch::LastChanges() {
  DLOG(INFO)<<"Opening blocking connection to URL: "<<url_;
  auto r = cpr::Get(
    cpr::Url { url_ },
    cpr::Parameters {{ "index", std::to_string(index_) }}
  );
  switch (r.status_code) {
    case 200: break;
    case 0:
      throw std::runtime_error("Can't contact Consul (host is unreachable)");
    case 404:
      throw std::runtime_error("Key doesn't exist: " + key_);
    default:
      throw std::runtime_error("Can't watch key (" + r.text + ")");
  }
  json response = json::parse(r.text)[0].get<json>();
  index_ = response["ModifyIndex"].get<long>();
  return response;
}

}}}
