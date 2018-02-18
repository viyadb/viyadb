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

#ifndef VIYA_CLUSTER_QUERY_QUERY_H_
#define VIYA_CLUSTER_QUERY_QUERY_H_

#include "util/config.h"
#include <memory>
#include <vector>

namespace viya {
namespace cluster {

class Controller;
class Partitioning;
class Plan;

} // namespace cluster
} // namespace viya

namespace viya {
namespace cluster {
namespace query {

namespace util = viya::util;

class ClusterQuery {
public:
  ClusterQuery(const util::Config &query) : query_(query) {}
  virtual ~ClusterQuery() {}

  const util::Config &query() const { return query_; }
  virtual std::string GetRedirectWorker() const { return std::string(); }

  virtual void Accept(class ClusterQueryVisitor &visitor) const = 0;

protected:
  const util::Config &query_;
};

class RemoteQuery : public ClusterQuery {
public:
  RemoteQuery(const util::Config &query, const Controller &controller);
  RemoteQuery(const util::Config &query, const Partitioning &partitioning,
              const Plan &plan);

  const std::vector<std::vector<std::string>> &target_workers() const {
    return target_workers_;
  }

  std::string GetRedirectWorker() const override;

  void Accept(class ClusterQueryVisitor &visitor) const;

private:
  void FindTargetWorkers();

private:
  const Partitioning &partitioning_;
  const Plan &plan_;
  std::vector<std::vector<std::string>> target_workers_;
};

class LocalQuery : public ClusterQuery {
public:
  LocalQuery(const util::Config &query) : ClusterQuery(query) {}

  void Accept(class ClusterQueryVisitor &visitor) const;
};

class LoadQuery : public ClusterQuery {
public:
  LoadQuery(const util::Config &query) : ClusterQuery(query) {}

  void Accept(class ClusterQueryVisitor &visitor) const;
};

class ClusterQueryVisitor {
public:
  virtual void Visit(const RemoteQuery *query __attribute__((unused))) const {};
  virtual void Visit(const LocalQuery *query __attribute__((unused))) const {};
  virtual void Visit(const LoadQuery *query __attribute__((unused))) const {};
};

class ClusterQueryFactory {
public:
  static std::unique_ptr<ClusterQuery> Create(const util::Config &query,
                                              const Controller &controller);
};

} // namespace query
} // namespace cluster
} // namespace viya

#endif // VIYA_CLUSTER_QUERY_QUERY_H_
