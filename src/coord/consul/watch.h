#ifndef VIYA_COORD_WATCH_H_
#define VIYA_COORD_WATCH_H_

#include <json.hpp>

namespace viya {
namespace coord {

using json = nlohmann::json;

class Consul;

class Watch {
  public:
    Watch(const Consul& consul, const std::string& key);
    Watch(const Watch& other) = delete;

    json LastChanges();

  private:
    const Consul& consul_;
    const std::string key_;
    const std::string url_;
    long index_;
};

}}

#endif // VIYA_COORD_WATCH_H_
