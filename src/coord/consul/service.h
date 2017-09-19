#ifndef VIYA_COORD_SERVICE_H_
#define VIYA_COORD_SERVICE_H_

#include <memory>
#include <json.hpp>
#include "util/config.h"
#include "util/schedule.h"

namespace viya {
namespace coord {

using json = nlohmann::json;

class Consul;

class Service {
  enum Status { OK, FAIL, WARN };

  public:
    Service(const Consul& consul, const std::string& name, uint16_t port, uint32_t ttl_sec, bool auto_hc);
    Service(const Service& other) = delete;
    ~Service();

    void Notify(Status status, const std::string& message);

  private:
    void Register();
    void Deregister();

  private:
    const Consul& consul_;
    const std::string name_;
    const uint16_t port_;
    const uint32_t ttl_sec_;
    std::string id_;
    json data_;
    std::unique_ptr<util::Repeat> repeat_;
};

}}

#endif // VIYA_COORD_SERVICE_H_
