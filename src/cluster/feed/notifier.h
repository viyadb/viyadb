#ifndef VIYA_CLUSTER_CONSUL_NOTIFIER_H_
#define VIYA_CLUSTER_CONSUL_NOTIFIER_H_

#include <functional>
#include <vector>
#include <json.hpp>

namespace viya { namespace util { class Config; }}

namespace viya {
namespace cluster {
namespace feed {

using json = nlohmann::json;

class Notifier {
  public:
    virtual ~Notifier() = default;
    virtual void Listen(std::function<void(const json& info)> callback) = 0;
    virtual std::vector<json> GetAllMessages() = 0;
    virtual json GetLastMessage() = 0;
};

class NotifierFactory {
  public:
    static Notifier* Create(const util::Config& notifier_conf);
};

}}}

#endif // VIYA_CLUSTER_CONSUL_NOTIFIER_H_
