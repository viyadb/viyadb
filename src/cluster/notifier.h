#ifndef VIYA_CLUSTER_NOTIFIER_H_
#define VIYA_CLUSTER_NOTIFIER_H_

#include <functional>
#include <vector>
#include <memory>

namespace viya { namespace util { class Config; }}

namespace viya {
namespace cluster {

class Info;

enum IndexerType { REALTIME, BATCH };

class Notifier {
  public:
    virtual ~Notifier() = default;
    virtual void Listen(std::function<void(const Info& info)> callback) = 0;
    virtual std::vector<std::unique_ptr<Info>> GetAllMessages() = 0;
    virtual std::unique_ptr<Info> GetLastMessage() = 0;
};

class NotifierFactory {
  public:
    static Notifier* Create(const util::Config& notifier_conf, IndexerType indexer_type);
};

class InfoFactory {
  public:
    static std::unique_ptr<Info> Create(const std::string& info, IndexerType indexer_type);
};

}}

#endif // VIYA_CLUSTER_NOTIFIER_H_
