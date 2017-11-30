#ifndef VIYA_CLUSTER_CONSUL_LISTENER_H_
#define VIYA_CLUSTER_CONSUL_LISTENER_H_

#include <functional>
#include <json.hpp>

namespace viya { namespace util { class Config; }}

namespace viya {
namespace cluster {
namespace feed {

using json = nlohmann::json;

class Listener {
  public:
    virtual ~Listener() = default;
    virtual void Start(std::function<void(const json& info)> callback) = 0;
};

class ListenerFactory {
  public:
    static Listener* Create(const util::Config& notifier_conf);
};

}}}

#endif // VIYA_CLUSTER_CONSUL_LISTENER_H_
