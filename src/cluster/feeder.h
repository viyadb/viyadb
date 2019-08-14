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

#ifndef VIYA_CLUSTER_FEEDER_H_
#define VIYA_CLUSTER_FEEDER_H_

#include "cluster/batch_info.h"
#include "cluster/loader.h"
#include "cluster/notifier.h"
#include "util/config.h"
#include "util/macros.h"
#include <map>
#include <vector>

namespace viya {
namespace cluster {

class Controller;

} // namespace cluster
} // namespace viya

namespace viya {
namespace cluster {

class Feeder : public MessageProcessor {
public:
  Feeder(const Controller &controller, const std::string &load_prefix);
  DISALLOW_COPY_AND_MOVE(Feeder);
  ~Feeder();

  bool ProcessMessage(const std::string &indexer_id, const Message &message);
  void ReloadWorker(const std::string &worker_id);
  void LoadData(const util::Config &load_desc,
                const std::string &worker_id = std::string());

protected:
  void Start();

  bool IsNewMicroBatch(const std::string &indexer_id,
                       const MicroBatchInfo &mb_info);

  void LoadHistoricalData(const std::string &target_worker = std::string());

  void LoadMicroBatch(const MicroBatchInfo &mb_info,
                      const std::string &target_worker = std::string());

  util::Config GetLoadDesc(const std::string &file,
                           const std::string &table_name,
                           const std::vector<std::string> &columns,
                           long batch_id);

private:
  const Controller &controller_;
  Loader loader_;
  std::vector<Notifier *> notifiers_;
};

} // namespace cluster
} // namespace viya

#endif // VIYA_CLUSTER_FEEDER_H_
