#include <glog/logging.h>
#include <cppkafka/consumer.h>
#include "util/config.h"
#include "util/hostname.h"
#include "util/schedule.h"
#include "cluster/feed/kafka_listener.h"

namespace viya {
namespace cluster {
namespace feed {

KafkaListener::KafkaListener(const util::Config& notifier_conf) {
  auto brokers = notifier_conf.str("channel");
  cppkafka::Configuration consumer_config = {
    {"metadata.broker.list", brokers},
    {"enable.auto.commit", false},
    {"group.id", "viyadb-feed-" + util::get_hostname()}
  };

  consumer_config.set_default_topic_configuration({
    {"auto.offset.reset", "smallest"}
  });

  auto topic = notifier_conf.str("topic");
  consumer_ = std::make_unique<cppkafka::Consumer>(consumer_config);
  consumer_->subscribe({ topic });
  LOG(INFO)<<"Subscribed to topic: "<<topic<<" on Kafka brokers: "<<brokers;
}

void KafkaListener::Start(std::function<void(const json& info)> callback) {
  always_ = std::make_unique<util::Always>([this, &callback]() {
    auto msg = consumer_->poll();
    if (msg) {
      if (msg.get_error()) {
        if (!msg.is_eof()) {
          LOG(WARNING)<<"Error occurred while consuming messages: "<<msg.get_error();
        }
      } else {
        auto info = json::parse(msg.get_payload());
        DLOG(INFO)<<"Received new message: "<<info.dump();
        try {
          callback(info);
          consumer_->commit(msg);
        } catch (std::exception& e) {
          LOG(ERROR)<<"Error processing Kafka message: "<<e.what();
          throw e;
        }
      }
    }
  });
}

}}}
