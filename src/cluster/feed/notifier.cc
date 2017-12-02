#include "util/config.h"
#include "cluster/feed/notifier.h"
#include "cluster/feed/kafka_notifier.h"

namespace viya {
namespace cluster {
namespace feed {

Notifier* NotifierFactory::Create(const util::Config& notifier_conf) {
  auto type = notifier_conf.str("type");
  Notifier* notifier = nullptr;
  if (type == "kafka") {
    return new KafkaNotifier(notifier_conf);
  } else {
    throw new std::runtime_error("Unsupported notifier type: " + type);
  }
  return notifier;
}

}}}
