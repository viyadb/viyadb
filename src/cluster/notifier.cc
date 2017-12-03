#include "util/config.h"
#include "cluster/notifier.h"
#include "cluster/kafka_notifier.h"

namespace viya {
namespace cluster {

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

}}
