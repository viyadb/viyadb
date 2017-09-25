#include <string>
#include <unistd.h>
#include <limits.h>
#include "util/hostname.h"

namespace viya {
namespace util {

std::string get_hostname() {
  char hostname[HOST_NAME_MAX];
  gethostname(hostname, HOST_NAME_MAX);
  return std::string(hostname);
}

}}
