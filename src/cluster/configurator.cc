#include <cpr/cpr.h>
#include <json.hpp>
#include "util/config.h"
#include "cluster/controller.h"
#include "cluster/configurator.h"

namespace viya {
namespace cluster {

using json = nlohmann::json;

Configurator::Configurator(const Controller& controller, const std::string& load_prefix):
  controller_(controller),
  load_prefix_(load_prefix) {
}

void Configurator::ConfigureWorkers() {
  for (auto& it : controller_.workers_configs()) {
    auto& worker_config = it.second;
    CreateTables(worker_config);
  }
}

void Configurator::AdaptTableConfig(util::Config& table_config) {
  json* raw_config = reinterpret_cast<json*>(table_config.json_ptr());
  for (auto& metric : (*raw_config)["metrics"]) {
    if (metric["type"] == "count") {
      metric["type"] = "long_sum";
    }
  }
}

void Configurator::CreateTables(const util::Config& worker_config) {
  std::string url = "http://" + worker_config.str("hostname") + ":"
    + std::to_string(worker_config.num("http_port")) + "/tables";

  for (auto& it : controller_.tables_configs()) {
    util::Config table_config(it.second);
    AdaptTableConfig(table_config);

    auto r = cpr::Post(
      cpr::Url { url },
      cpr::Body { table_config.dump() },
      cpr::Header {{ "Content-Type", "application/json" }},
      cpr::Timeout { 3000L }
    );
    if (r.status_code != 201) {
      if (r.status_code == 0) {
        throw std::runtime_error("Can't contact worker at: " + url + " (host is unreachable)");
      }
      throw std::runtime_error("Can't create table in worker (" + r.text + ")");
    }
  }
}

}}
