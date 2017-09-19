#ifndef VIYA_SERVER_VIYAD_H_
#define VIYA_SERVER_VIYAD_H_

#include "util/config.h"
#include "coord/consul/consul.h"
#include "server/http/service.h"

namespace viya {
namespace server {

namespace util = viya::util;

class Viyad {
  public:
    Viyad(const util::Config& config);

    void Start();

 private:
#ifdef __linux__
    void SetCpuAffinity();
#endif
    void RegisterNode(const server::Http& http_service);

  private:
    const util::Config config_;
    const coord::Consul consul_;
    std::unique_ptr<coord::Session> session_;
};

}}

#endif // VIYA_SERVER_VIYAD_H_
