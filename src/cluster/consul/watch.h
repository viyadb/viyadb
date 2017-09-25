#ifndef VIYA_CLUSTER_WATCH_H_
#define VIYA_CLUSTER_WATCH_H_

#include <json.hpp>

namespace viya {
namespace cluster {

using json = nlohmann::json;

class Consul;

class Watch {
  public:
    Watch(const Consul& consul, const std::string& key);
    Watch(const Watch& other) = delete;

    json LastChanges();

  private:
    const Consul& consul_;
    const std::string key_;
    const std::string url_;
    long index_;
};

}}

#endif // VIYA_CLUSTER_WATCH_H_
