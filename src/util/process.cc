#include <cpp-subprocess/subprocess.hpp>
#include "util/process.h"

namespace viya {
namespace util {

namespace sp = subprocess;

int Process::Run(const std::vector<std::string>& cmd) {
  return sp::Popen(cmd).wait();
}

int Process::RunWithInput(const std::vector<std::string>& cmd, const std::string& input) {
  auto p = sp::Popen(cmd, sp::input {sp::PIPE});
  p.communicate(input.c_str(), input.size());
  return p.wait();
}

}}
