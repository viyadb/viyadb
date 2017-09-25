#ifndef VIYA_SERVER_VIYAD_H_
#define VIYA_SERVER_VIYAD_H_

#include "util/config.h"
#include "cluster/worker.h"
#include "server/http/service.h"

namespace viya {
namespace server {

namespace util = viya::util;
namespace cluster = viya::cluster;

class Viyad {
  public:
    Viyad(const util::Config& config);

    void Start();

 private:
#ifdef __linux__
    void SetCpuAffinity();
#endif

  private:
    const util::Config config_;
    std::unique_ptr<cluster::Worker> worker_;
};

}}

#endif // VIYA_SERVER_VIYAD_H_
