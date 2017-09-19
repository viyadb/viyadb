#ifndef VIYA_UTIL_SANITIZE_H_
#define VIYA_UTIL_SANITIZE_H_

#include <string>

namespace viya {
namespace util {

/**
 * Checks whether the string can be safely used when generating a C++ code
 */
void check_legal_string(const std::string& what, const std::string& str);

}}

#endif // VIYA_UTIL_SANITIZE_H_
