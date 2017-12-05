#ifndef VIYA_UTIL_COMMAND_H
#define VIYA_UTIL_COMMAND_H

#include <vector>

namespace viya {
namespace util {

class Process {
  public:
    static int Run(const std::vector<std::string>& cmd);
    static int RunWithInput(const std::vector<std::string>& cmd, const std::string& input);
};

}}

#endif // VIYA_UTIL_COMMAND_H
