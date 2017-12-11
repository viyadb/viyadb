#include <stdexcept>
#include "db/database.h"
#include "input/loader_factory.h"
#include "input/file.h"
#include "util/config.h"

namespace viya {
namespace input {

Loader* LoaderFactory::Create(const util::Config& config, db::Database& database) {
  auto table = database.GetTable(config.str("table"));

  std::string type = config.str("type");
  if (type == "file") {
    return new FileLoader(config, *table);
  }
  throw std::invalid_argument("Unsupported input type: " + type);
}

}}
