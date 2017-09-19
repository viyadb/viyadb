#include "util/sanitize.h"
#include <stdexcept>
#include <algorithm>

namespace viya {
namespace util {

void check_legal_string(const std::string& what, const std::string& str) {
  std::string illegal_chars("\"\\");
  auto w = std::find_if(str.begin(), str.end(), [&illegal_chars] (char ch) {
    return illegal_chars.find(ch) != std::string::npos;
  });
  if(w != str.end()) {
    throw std::invalid_argument(
      what + " contains illegal character at (" + std::to_string(std::distance(str.begin(), w)) + "): " + str);
  }
}

}}
