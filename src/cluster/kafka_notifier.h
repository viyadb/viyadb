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

#ifndef VIYA_CLUSTER_KAFKA_NOTIFIER_H_
#define VIYA_CLUSTER_KAFKA_NOTIFIER_H_

#include <memory>
#include <map>
#include "cluster/notifier.h"

namespace cppkafka { class Consumer; class Configuration; }
namespace viya { namespace util { class Always; }}

namespace viya {
namespace cluster {

class KafkaNotifier: public Notifier {
  public:
    KafkaNotifier(const util::Config& config, IndexerType indexer_type);

    void Listen(std::function<void(const Info& info)> callback);
    std::vector<std::unique_ptr<Info>> GetAllMessages();
    std::unique_ptr<Info> GetLastMessage();

  protected:
    cppkafka::Configuration CreateConsumerConfig(const std::string& group_id);
    std::map<uint32_t, int64_t> GetLatestOffsets(cppkafka::Consumer& consumer);

  private:
    const std::string brokers_;
    const std::string topic_;
    IndexerType indexer_type_;
    std::unique_ptr<cppkafka::Consumer> consumer_;
    std::unique_ptr<util::Always> always_;
};

}}

#endif // VIYA_CLUSTER_KAFKA_NOTIFIER_H_
