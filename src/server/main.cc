#include <glog/logging.h>
#include "server/viyad.h"
#include "server/supervisor.h"

namespace server = viya::server;
namespace util = viya::util;

int main(int argc, char* argv[]) {
  try {
    FLAGS_logtostderr = 1;
    google::InitGoogleLogging(argv[0]);

    auto supervisor = std::make_unique<server::Supervisor>(std::vector<std::string>(argv, argv + argc));
    supervisor->Start();

  } catch (std::exception& e) {
    LOG(ERROR)<<e.what();
    exit(-1);
  }
}
