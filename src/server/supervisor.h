#ifndef VIYA_SERVER_SUPERVISOR_H_
#define VIYA_SERVER_SUPERVISOR_H_

#include <memory>
#include <sys/types.h>
#include <vector>
#include <csetjmp>
#include "coord/controller.h"
#include "util/config.h"

namespace viya {
namespace server {

namespace util = viya::util;

class Supervisor {
  public:
    struct Worker {
      pid_t pid;
      uint64_t last_start;
      uint8_t fast_failures = 0;
      util::Config config;
    };

  public:
    Supervisor(const std::vector<std::string>& args);

    void Start();
    void Stop();
    void Restart();

  private:
    void EnableRestartHandler();
    util::Config PrepareWorkerConfig(const util::Config& config, size_t worker_idx);
    void StartWorker(util::Config worker_config, Supervisor::Worker& info);

  private:
    static Supervisor* instance_;
    std::vector<std::string> args_;
    std::vector<Worker> workers_;
    std::unique_ptr<coord::Controller> controller_;
    std::jmp_buf jump_;
};

}}

#endif // VIYA_SERVER_SUPERVISOR_H_
