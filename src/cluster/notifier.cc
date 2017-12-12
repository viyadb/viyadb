/*
 * Copyright (c) 2017 ViyaDB Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
