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

#include <stack>
#include <unordered_map>
#include "query/filter.h"
#include "cluster/controller.h"
#include "cluster/query.h"
 
namespace viya {
namespace cluster {

namespace query = viya::query;

class FilterAnalyzer : public query::FilterVisitor {
  using ColumnsValues = std::unordered_map<std::string, std::vector<std::string>>;

  public:
    FilterAnalyzer(const query::Filter& filter);

    void Visit(const query::RelOpFilter* filter);
    void Visit(const query::InFilter* filter);
    void Visit(const query::CompositeFilter* filter);
    void Visit(const query::NotFilter* filter);
    void Visit(const query::EmptyFilter* filter);

  private:
    std::stack<ColumnsValues> stack_;
};

FilterAnalyzer::FilterAnalyzer(const query::Filter& filter) {
  stack_.emplace(FilterAnalyzer::ColumnsValues {});
  filter.Accept(*this);
}

void FilterAnalyzer::Visit(const query::RelOpFilter* filter) {
}

void FilterAnalyzer::Visit(const query::InFilter* filter) {
}

void FilterAnalyzer::Visit(const query::CompositeFilter* filter) {
  stack_.emplace(FilterAnalyzer::ColumnsValues {});
}

void FilterAnalyzer::Visit(const query::NotFilter* filter) {
}

void FilterAnalyzer::Visit(const query::EmptyFilter* filter) {
}

ClusterQuery::ClusterQuery(const Controller& controller, const util::Config& query):
  controller_(controller),query_(query) {
}

void ClusterQuery::BuildRemoteQueries() {
  if (query_.exists("filter")) {
    query::FilterFactory filter_factory;
    std::unique_ptr<query::Filter> filter(filter_factory.Create(query_.sub("filter")));
  }
}

}}

