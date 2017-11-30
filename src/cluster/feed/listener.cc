#include "util/config.h"
#include "cluster/feed/listener.h"
#include "cluster/feed/kafka_listener.h"

namespace viya {
namespace cluster {
namespace feed {

Listener* ListenerFactory::Create(const util::Config& notifier_conf) {
  auto type = notifier_conf.str("type");
  Listener* listener = nullptr;
  if (type == "kafka") {
    return new KafkaListener(notifier_conf);
  } else {
    throw new std::runtime_error("Unsupported notifier type: " + type);
  }
  return listener;
}

}}}
