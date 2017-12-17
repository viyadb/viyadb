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

#ifndef VIYA_CLUSTER_FEEDER_H_
#define VIYA_CLUSTER_FEEDER_H_

#include <vector>
#include <map>
#include "cluster/batch_info.h"
#include "cluster/loader.h"
#include "cluster/notifier.h"

namespace viya { namespace util { class Config; }}
namespace viya { namespace cluster { class Controller; }}

namespace viya {
namespace cluster {

class Feeder : public MessageProcessor {
  public:
    Feeder(const Controller& controller, const std::string& load_prefix);
    Feeder(const Feeder& other) = delete;
    ~Feeder();

    bool ProcessMessage(const std::string& indexer_id, const Message& message);

  protected:
    void Start();
    void LoadHistoricalData();

  private:
    const Controller& controller_;
    Loader loader_;
    std::vector<Notifier*> notifiers_;

};

}}

#endif // VIYA_CLUSTER_FEEDER_H_
