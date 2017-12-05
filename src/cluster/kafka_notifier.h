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
