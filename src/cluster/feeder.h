#ifndef VIYA_CLUSTER_FEEDER_H_
#define VIYA_CLUSTER_FEEDER_H_

#include <vector>
#include <json.hpp>

namespace viya { namespace util { class Config; }}
namespace viya { namespace cluster { class Controller; }}

namespace viya {
namespace cluster {

class Notifier;

using json = nlohmann::json;

class Feeder {
  public:
    Feeder(cluster::Controller& controller);
    Feeder(const Feeder& other) = delete;
    ~Feeder();

  protected:
    void Start();
    void ProcessMicroBatch(const json& info);

  private:
    cluster::Controller& controller_;
    std::vector<Notifier*> notifiers_;
};

}}

#endif // VIYA_CLUSTER_FEEDER_H_
