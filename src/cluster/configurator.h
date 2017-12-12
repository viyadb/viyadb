/*
 * Copyright (c) 2017 ViyaDB Group
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

#ifndef VIYA_CLUSTER_CONFIGURATOR_H_
#define VIYA_CLUSTER_CONFIGURATOR_H_

namespace viya { namespace util { class Config; }}
namespace viya { namespace cluster { class Controller; }}

namespace viya {
namespace cluster {

/**
 * Configures workers on this node
 */
class Configurator {
  public:
    Configurator(const Controller& controller, const std::string& load_prefix_);
    Configurator(const Configurator& other) = delete;

    void ConfigureWorkers();

  protected:
    void AdaptTableConfig(util::Config& table_config);
    void CreateTables(const util::Config& worker_config);

  private:
    const Controller& controller_;
    const std::string load_prefix_;
};

}}

#endif // VIYA_CLUSTER_CONFIGURATOR_H_
