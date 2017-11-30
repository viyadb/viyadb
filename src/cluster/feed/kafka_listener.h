#ifndef VIYA_CLUSTER_CONSUL_KAFKA_LISTENER_H_
#define VIYA_CLUSTER_CONSUL_KAFKA_LISTENER_H_

#include <memory>
#include "cluster/feed/listener.h"

namespace cppkafka { class Consumer; }
namespace viya { namespace util { class Always; }}

namespace viya {
namespace cluster {
namespace feed {

class KafkaListener: public Listener {
  public:
    KafkaListener(const util::Config& notifier_conf);

    void Start(std::function<void(const json& info)> callback);

  private:
    std::unique_ptr<cppkafka::Consumer> consumer_;
    std::unique_ptr<util::Always> always_;
};

}}}

#endif // VIYA_CLUSTER_CONSUL_KAFKA_LISTENER_H_
