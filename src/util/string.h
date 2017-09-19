#ifndef VIYA_UTIL_STRING_H_
#define VIYA_UTIL_STRING_H_

#include <string>

namespace viya {
namespace util {

/**
 * This utility implements comparison methods for strings containing numbers
 */
class StringNumCmp {
  public:
    static bool GreaterInt(const std::string& str1, const std::string& str2) {
      size_t size1 = str1.size();
      size_t size2 = str2.size();
      return size1 != size2 ? (size1 > size2 ? true : false) : (str1 > str2);
    }

    static bool SmallerInt(const std::string& str1, const std::string& str2) {
      size_t size1 = str1.size();
      size_t size2 = str2.size();
      return size1 != size2 ? (size1 < size2 ? true : false) : (str1 < str2);
    }

    static bool GreaterFloat(const std::string& str1, const std::string& str2) {
      return std::stod(str1) > std::stod(str2);
    }

    static bool SmallerFloat(const std::string& str1, const std::string& str2) {
      return std::stod(str1) < std::stod(str2);
    }
};

}}

#endif // VIYA_UTIL_STRING_H_
