#ifndef VIYA_SERVER_ARGS_H_
#define VIYA_SERVER_ARGS_H_

#include <vector>
#include "util/config.h"

namespace viya {
namespace server {

namespace util = viya::util;

class CmdlineArgs {
  public:
    CmdlineArgs() {}
    util::Config Parse(std::vector<std::string> args);

  private:
    std::string Help();
    util::Config Defaults();
    util::Config OpenConfig(const std::string& file);
};

}}

#endif // VIYA_SERVER_ARGS_H_
