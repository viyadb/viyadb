#include <cpr/cpr.h>
#include <glog/logging.h>
#include "cluster/consul/consul.h"
#include "cluster/consul/watch.h"

namespace viya {
namespace cluster {

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
  if (r.status_code != 200) {
    if (r.status_code == 0) {
      throw std::runtime_error("Can't contact Consul (host is unreachable)");
    }
    if (r.status_code == 404) {
      throw std::runtime_error("Key doesn't exist: " + key_);
    }
    throw std::runtime_error("Can't watch key (" + r.text + ")");
  }
  json response = json::parse(r.text)[0].get<json>();
  index_ = response["ModifyIndex"].get<long>();
  return std::move(response);
}

}}
