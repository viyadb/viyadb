#include <tuple>
#include <glog/logging.h>
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

KafkaNotifier::KafkaNotifier(const util::Config& config, IndexerType indexer_type):
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

  return std::move(consumer_config);
}

void KafkaNotifier::Listen(std::function<void(const Info& info)> callback) {
  auto config = CreateConsumerConfig("viyadb-" + util::get_hostname());
  consumer_ = std::make_unique<cppkafka::Consumer>(config);
  consumer_->subscribe({ topic_ });

  LOG(INFO)<<"Subscribed to topic: "<<topic_<<" on Kafka brokers: "<<brokers_;

  always_ = std::make_unique<util::Always>([this, &callback]() {
    auto msg = consumer_->poll();
    if (msg) {
      if (msg.get_error()) {
        if (!msg.is_eof()) {
          LOG(WARNING)<<"Error occurred while consuming messages: "<<msg.get_error();
        }
      } else {
        auto& payload = msg.get_payload();
        DLOG(INFO)<<"Received new message: "<<payload;
        auto info = InfoFactory::Create(payload, indexer_type_);
        try {
          callback(*info);
          consumer_->commit(msg);
        } catch (std::exception& e) {
          LOG(ERROR)<<"Error processing Kafka message: "<<e.what();
          throw e;
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

std::vector<std::unique_ptr<Info>> KafkaNotifier::GetAllMessages() {
  auto config = CreateConsumerConfig("viyadb-tmp-" + util::get_hostname());
  cppkafka::Consumer consumer(config);
  auto latest_offsets = GetLatestOffsets(consumer);
  std::vector<std::unique_ptr<Info>> messages;

  consumer.subscribe({ topic_ });

  // Read all messages until latest partition offsets are meet:
  while (!latest_offsets.empty()) {
    auto msg = consumer.poll();
    if (msg) {
      if (msg.get_error()) {
        if (!msg.is_eof()) {
          LOG(WARNING)<<"Error occurred while consuming messages: "<<msg.get_error();
        }
      } else {
        messages.emplace_back(std::move(InfoFactory::Create(msg.get_payload(), indexer_type_)));
      }
      auto msg_partition = msg.get_partition();
      if (latest_offsets[msg_partition] <= msg.get_offset()) {
        latest_offsets.erase(msg_partition);
      }
    }
  }
  return std::move(messages);
}

std::unique_ptr<Info> KafkaNotifier::GetLastMessage() {
  auto config = CreateConsumerConfig("viyadb-tmp-" + util::get_hostname());
  cppkafka::Consumer consumer(config);
  auto latest_offsets = GetLatestOffsets(consumer);
  auto latest_offset = std::max_element(latest_offsets.begin(), latest_offsets.end(),
                                        [](auto& p1, auto& p2) { return p1.second < p2.second; });

  // Only assign the partition containing the latest offset:
  if (latest_offset != latest_offsets.end()) {
    consumer.assign({{topic_, (int)latest_offset->first}});
  }

  std::unique_ptr<Info> last_message;
  while (true) {
    auto msg = consumer.poll();
    if (msg) {
      if (msg.get_error()) {
        if (!msg.is_eof()) {
          LOG(WARNING)<<"Error occurred while consuming messages: "<<msg.get_error();
        }
      } else {
        last_message = std::move(InfoFactory::Create(msg.get_payload(), indexer_type_));
      }
      if (latest_offset->second <= msg.get_offset()) {
        break;
      }
    }
  }
  return std::move(last_message);
}

}}
