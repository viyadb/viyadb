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

#ifndef VIYA_QUERY_FILTER_H_
#define VIYA_QUERY_FILTER_H_

#include <string>
#include <vector>

namespace viya { namespace util { class Config; }}

namespace viya {
namespace query {

namespace util = viya::util;

class FilterVisitor;

class Filter {
  public:
    Filter(int precedence):precedence_(precedence) {}
    Filter(const Filter& other) = delete;
    virtual ~Filter() {}

    int precedence() const { return precedence_; }

    virtual void Accept(FilterVisitor& visitor) const = 0;

  private:
    const int precedence_;
};

class RelOpFilter: public Filter {
  public:
    enum Operator { EQUAL = 0, NOT_EQUAL, LESS, LESS_EQUAL, GREATER, GREATER_EQUAL };

    RelOpFilter(const Operator& op, const std::string& column, const std::string& value)
      :Filter(1),op_(op),column_(column),value_(value) {
    }

    RelOpFilter(const RelOpFilter& other) = delete;

    Operator op() const { return op_; }
    const char* opstr() const { return opstr_[op_]; }
    const std::string& column() const { return column_; }
    const std::string& value() const { return value_; }

    void Accept(FilterVisitor& visitor) const;

  private:
    Operator op_;
    const char* opstr_[6] = {"==", "!=", "<", "<=", ">", ">="};
    const std::string column_;
    const std::string value_;
};

class InFilter: public Filter {
  public:
    InFilter(const std::string& column, const std::vector<std::string>& values, bool equal = true)
      :Filter(4),column_(column),values_(values),equal_(equal) {}

    InFilter(const InFilter& other) = delete;

    const std::string& column() const { return column_; }
    const std::vector<std::string>& values() const { return values_; }
    bool equal() const { return equal_; }

    void Accept(FilterVisitor& visitor) const;

  private:
    const std::string column_;
    const std::vector<std::string> values_;
    const bool equal_;
};

class CompositeFilter: public Filter {
  public:
    enum Operator { AND, OR };

    CompositeFilter(Operator op, const std::vector<Filter*>& filters)
      :Filter(op == Operator::AND ? 2 : 3),op_(op),filters_(filters) {}

    CompositeFilter(const CompositeFilter& other) = delete;

    ~CompositeFilter() {
      for (auto filter: filters_) {
        delete filter;
      }
    }

    Operator op() const { return op_; }
    const std::vector<Filter*>& filters() const { return filters_; }

    void Accept(FilterVisitor& visitor) const;

  private:
    Operator op_;
    const std::vector<Filter*> filters_;
};

class EmptyFilter: public Filter {
  public:
    EmptyFilter():Filter(0) {}
    EmptyFilter(const EmptyFilter& other) = delete;

    void Accept(FilterVisitor& visitor) const;
};

class FilterVisitor {
  public:
    virtual ~FilterVisitor() {}

    virtual void Visit(const RelOpFilter* filter) = 0;
    virtual void Visit(const InFilter* filter) = 0;
    virtual void Visit(const CompositeFilter* filter) = 0;
    virtual void Visit(const EmptyFilter* filter) = 0;
};

class FilterFactory {
  public:
    Filter* Create(const util::Config& config, bool negate=false);
};

}}

#endif // VIYA_QUERY_FILTER_H_
