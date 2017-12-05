#include "util/config.h"
#include "cluster/notifier.h"
#include "cluster/kafka_notifier.h"
#include "cluster/batch_info.h"

namespace viya {
namespace cluster {

std::unique_ptr<Info> InfoFactory::Create(const std::string& info, IndexerType indexer_type) {
  if (indexer_type == IndexerType::REALTIME) {
    return std::move(std::make_unique<MicroBatchInfo>(info));
  }
  return std::move(std::make_unique<BatchInfo>(info));
}

Notifier* NotifierFactory::Create(const util::Config& notifier_conf, IndexerType indexer_type) {
  auto type = notifier_conf.str("type");
  Notifier* notifier = nullptr;
  if (type == "kafka") {
    return new KafkaNotifier(notifier_conf, indexer_type);
  } else {
    throw new std::runtime_error("Unsupported notifier type: " + type);
  }
  return notifier;
}

}}
