/*
 * Copyright (c) 2017-present ViyaDB Group
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cluster/configurator.h"
#include "cluster/controller.h"
#include "util/config.h"
#include "util/hostname.h"
#include <cpr/cpr.h>
#include <glog/logging.h>
#include <nlohmann/json.hpp>
#include <util/hostname.h>

namespace viya {
namespace cluster {

using json = nlohmann::json;

Configurator::Configurator(const Controller &controller)
    : controller_(controller) {}

void Configurator::ConfigureWorkers() {
  auto hostname = util::get_hostname();
  for (auto &plans_it : controller_.tables_plans()) {
    auto &table_name = plans_it.first;
    auto &plan = plans_it.second;
    for (auto &replicas : plan.partitions()) {
      for (auto &placement : replicas) {
        if (hostname == placement.hostname()) {
          CreateTable(controller_.tables_configs().at(table_name),
                      placement.hostname(), placement.port());
        }
      }
    }
  }
}

void Configurator::CreateTable(const util::Config &table_config,
                               const std::string &hostname, uint16_t port) {
  std::string url =
      "http://" + hostname + ":" + std::to_string(port) + "/tables";
  auto data = table_config.dump();
  auto r = cpr::Post(
      cpr::Url{url}, cpr::Body{data},
      cpr::Header{{"Content-Type", "application/json"}, {"Expect", "None"}},
      cpr::Timeout{3000L});
  if (r.status_code != 201) {
    if (r.status_code == 0) {
      throw std::runtime_error("Can't contact worker at: " + url +
                               " (host is unreachable)");
    }
    throw std::runtime_error("Can't create table in worker (" + r.text + ")");
  }
}

} // namespace cluster
} // namespace viya
