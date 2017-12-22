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

#include <tuple>
#include <glog/logging.h>
#include <boost/exception/diagnostic_information.hpp>
#include <cppkafka/consumer.h>
#include <cppkafka/metadata.h>
#include <cppkafka/topic.h>
#include "util/config.h"
#include "util/hostname.h"
#include "util/schedule.h"
#include "cluster/batch_info.h"
#include "cluster/kafka_notifier.h"

namespace viya {
namespace cluster {

KafkaNotifier::KafkaNotifier(const std::string& indexer_id,
                             const util::Config& config, IndexerType indexer_type):
  indexer_id_(indexer_id),
  brokers_(config.str("channel")),
  topic_(config.str("queue")),
  indexer_type_(indexer_type) {
}

cppkafka::Configuration KafkaNotifier::CreateConsumerConfig(const std::string& group_id) {
  cppkafka::Configuration consumer_config({
    {"metadata.broker.list", brokers_},
    {"enable.auto.commit", false},
    {"group.id", group_id}
  });

  consumer_config.set_default_topic_configuration({
    {"auto.offset.reset", "smallest"}
  });

  return consumer_config;
}

void KafkaNotifier::Listen(MessageProcessor& processor) {
  auto config = CreateConsumerConfig("viyadb-" + util::get_hostname());
  consumer_ = std::make_unique<cppkafka::Consumer>(config);

  consumer_->subscribe({ topic_ });
  LOG(INFO)<<"Start listening [topic="<<topic_<<",brokers="<<brokers_<<"]";

  always_ = std::make_unique<util::Always>([this, &processor]() {
    auto msg = consumer_->poll();
    if (msg) {
      if (msg.get_error()) {
        if (!msg.is_eof()) {
          LOG(WARNING)<<"Error occurred while consuming messages: "<<msg.get_error();
        }
      } else {
        auto& payload = msg.get_payload();
        LOG(INFO)<<"Received new message: "<<payload;
        auto message = MessageFactory::Create(payload, indexer_type_);
        if (processor.ProcessMessage(indexer_id_, *message)) {
          consumer_->commit(msg);
        }
      }
    }
  });
}

std::map<uint32_t, int64_t> KafkaNotifier::GetLatestOffsets(cppkafka::Consumer& consumer) {
  std::map<uint32_t, int64_t> last_offsets;
  auto metadata = consumer.get_metadata(consumer.get_topic(topic_));
  for (auto& partition : metadata.get_partitions()) {
    auto offsets = consumer.query_offsets({topic_, (int)partition.get_id()});
    auto last_offset = std::get<1>(offsets);
    if (last_offset > 0) {
      // Only return offsets that point to an existing message:
      last_offsets[partition.get_id()] = last_offset - 1;
    }
  }
  return std::move(last_offsets);
}

std::vector<std::unique_ptr<Message>> KafkaNotifier::GetAllMessages() {
  auto config = CreateConsumerConfig("viyadb-tmp-" + util::get_hostname());
  cppkafka::Consumer consumer(config);
  auto latest_offsets = GetLatestOffsets(consumer);
  std::vector<std::unique_ptr<Message>> messages;

  consumer.subscribe({ topic_ });
  LOG(INFO)<<"Reading all messages [topic="<<topic_<<",brokers="<<brokers_<<"]";

  // Read all messages until latest partition offsets are meet:
  while (!latest_offsets.empty()) {
    auto msg = consumer.poll();
    if (msg) {
      if (msg.get_error()) {
        if (!msg.is_eof()) {
          LOG(WARNING)<<"Error occurred while consuming messages: "<<msg.get_error();
        }
      } else {
        messages.emplace_back(std::move(MessageFactory::Create(msg.get_payload(), indexer_type_)));
      }
      auto msg_partition = msg.get_partition();
      if (latest_offsets[msg_partition] <= msg.get_offset()) {
        latest_offsets.erase(msg_partition);
      }
    }
  }

  LOG(INFO)<<"Read "<<messages.size()<<" messages";
  return std::move(messages);
}

std::unique_ptr<Message> KafkaNotifier::GetLastMessage() {
  auto config = CreateConsumerConfig("viyadb-tmp-" + util::get_hostname());
  cppkafka::Consumer consumer(config);
  auto latest_offsets = GetLatestOffsets(consumer);
  auto latest_offset = std::max_element(latest_offsets.begin(), latest_offsets.end(),
                                        [](auto& p1, auto& p2) { return p1.second < p2.second; });

  std::unique_ptr<Message> last_message;
  LOG(INFO)<<"Looking for last message [topic="<<topic_<<",brokers="<<brokers_<<"]";

  // Only assign the partition containing the latest offset:
  if (latest_offset != latest_offsets.end()) {
    auto last_partition = (int)latest_offset->first;
    consumer.assign({{topic_, last_partition}});

    LOG(INFO)<<"Start reading from partition: "<<last_partition;
    while (true) {
      auto msg = consumer.poll();
      if (msg) {
        if (msg.get_error()) {
          if (!msg.is_eof()) {
            LOG(WARNING)<<"Error occurred while consuming messages: "<<msg.get_error();
          }
        } else {
          auto& payload = msg.get_payload();
          LOG(INFO)<<"Read last message: "<<payload;
          last_message = std::move(MessageFactory::Create(payload, indexer_type_));
        }
        if (latest_offset->second <= msg.get_offset()) {
          break;
        }
      }
    }
  } else {
    LOG(INFO)<<"No messages available";
  }
  return std::move(last_message);
}

}}
