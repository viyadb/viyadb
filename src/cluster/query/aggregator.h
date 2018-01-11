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

#ifndef VIYA_CLUSTER_QUERY_AGGREGATOR_H_
#define VIYA_CLUSTER_QUERY_AGGREGATOR_H_

#include "cluster/query/query.h"
#include "util/config.h"
#include <vector>

namespace viya {
namespace cluster {
class Controller;
}
}
namespace viya {
namespace query {
class RowOutput;
}
}

namespace viya {
namespace cluster {
namespace query {

class ClusterQuery;

namespace util = viya::util;
namespace query = viya::query;

class Aggregator : public ClusterQueryVisitor {
public:
  Aggregator(Controller &controller, query::RowOutput &output);

  void Visit(const RemoteQuery *query);
  void Visit(const LocalQuery *query);

  const std::string &redirect_worker() const { return redirect_worker_; }

protected:
  std::string CreateTempTable(const util::Config &worker_query);
  util::Config CreateWorkerQuery(const RemoteQuery *cluster_query);
  void ShowWorkers();

private:
  Controller &controller_;
  query::RowOutput &output_;
  std::string redirect_worker_;
};
}
}
}

#endif // VIYA_CLUSTER_QUERY_AGGREGATOR_H_
