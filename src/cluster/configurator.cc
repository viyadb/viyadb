#include <cpr/cpr.h>
#include "util/config.h"
#include "cluster/controller.h"
#include "cluster/configurator.h"

namespace viya {
namespace cluster {

Configurator::Configurator(const Controller& controller, const std::string& load_prefix):
  controller_(controller),
  load_prefix_(load_prefix) {
}

void Configurator::ConfigureWorkers() {
  for (auto& it : controller_.workers_configs()) {
    auto& worker_id = it.first;
    auto& worker_config = it.second;
    CreateTables(worker_id, worker_config);
  }
}

void Configurator::AdaptTableConfig(const std::string& worker_id, util::Config& table_config) {
  util::Config watch_config;
  std::string watch_dir = load_prefix_ + "/watch/" + worker_id + "/" + table_config.str("name");
  watch_config.set_str("directory", watch_dir.c_str());
  watch_config.set_strlist("extensions", std::vector<std::string> {".tsv"});
  table_config.set_sub("watch", watch_config);
}

void Configurator::CreateTables(const std::string& worker_id, const util::Config& worker_config) {
  std::string url = "http://" + worker_config.str("hostname") + ":"
    + std::to_string(worker_config.num("http_port")) + "/tables";

  for (auto& it : controller_.tables_configs()) {
    util::Config table_config(it.second);
    AdaptTableConfig(worker_id, table_config);

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
