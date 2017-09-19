#include <sstream>
#include <unordered_set>
#include "codegen/generator.h"

namespace viya {
namespace codegen {

Code::Code(Code&& other) {
  headers_ = std::move(other.headers_);  
  body_ = std::move(other.body_);
}

std::string Code::str() const {
  std::stringstream ss;
  std::unordered_set<std::string> headers_set(headers_.begin(), headers_.end());
  for (auto& header : headers_set) {
    ss<<"#include <"<<header<<">"<<std::endl;
  }
  ss<<body_.str();
  return ss.str();
}

}}
