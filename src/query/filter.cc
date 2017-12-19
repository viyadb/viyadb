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

#include <algorithm>
#include <stdexcept>
#include "util/config.h"
#include "query/filter.h"

namespace viya {
namespace query {

void RelOpFilter::Accept(FilterVisitor& visitor) const {
  visitor.Visit(this);
}

void InFilter::Accept(FilterVisitor& visitor) const {
  visitor.Visit(this);
}

void CompositeFilter::Accept(FilterVisitor& visitor) const {
  visitor.Visit(this);
}

void NotFilter::Accept(FilterVisitor& visitor) const {
  visitor.Visit(this);
}

void EmptyFilter::Accept(FilterVisitor& visitor) const {
  visitor.Visit(this);
}

Filter* FilterFactory::Create(const util::Config& config) {
  if (!config.exists("op")) {
    return new EmptyFilter();
  }
  std::string op = config.str("op");
  if (op == "and" || op == "or") {
    std::vector<Filter*> filters;
    for (util::Config& filter_conf : config.sublist("filters")) {
      filters.push_back(Create(filter_conf));
    }
    std::sort(filters.begin(), filters.end(), [] (const Filter* a, const Filter* b) -> bool { 
        return a->precedence() < b->precedence();
    });
    return new CompositeFilter(op == "and" ? CompositeFilter::Operator::AND
        : CompositeFilter::Operator::OR, filters);
  }
  if (op == "not") {
    return new NotFilter(Create(config.sub("filter")));
  }

  const auto column = config.str("column");
  if (op == "in") {
    auto values = config.strlist("values");
    return new InFilter(column, values);
  }

  auto value = config.str("value");
  if (op == "eq") {
    return new RelOpFilter(RelOpFilter::Operator::EQUAL, column, value);
  }
  if (op == "ne") {
    return new RelOpFilter(RelOpFilter::Operator::NOT_EQUAL, column, value);
  }
  if (op == "lt") {
    return new RelOpFilter(RelOpFilter::Operator::LESS, column, value);
  }
  if (op == "le") {
    return new RelOpFilter(RelOpFilter::Operator::LESS_EQUAL, column, value);
  }
  if (op == "gt") {
    return new RelOpFilter(RelOpFilter::Operator::GREATER, column, value);
  }
  if (op == "ge") {
    return new RelOpFilter(RelOpFilter::Operator::GREATER_EQUAL, column, value);
  }
  throw std::invalid_argument("Unsupported filter operataor: " + op);
}

}}

