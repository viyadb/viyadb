#include <vector>
#include "input/simple.h"
#include "util/config.h"

namespace viya {
namespace input {

SimpleLoader::SimpleLoader(const db::Table& table):
  Loader(util::Config {}, table) {
}

SimpleLoader::SimpleLoader(const util::Config& config, const db::Table& table):
  Loader(config, table) {
}

void SimpleLoader::Load(std::initializer_list<std::vector<std::string>> rows) {
  BeforeLoad();
  for (auto row : rows) {
    Loader::Load(row);
  }
  AfterLoad();
}

}}

