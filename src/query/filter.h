#ifndef VIYA_QUERY_FILTER_H_
#define VIYA_QUERY_FILTER_H_

#include <vector>

namespace viya {
namespace db {

class Column;
class Table;

}}

namespace viya {
namespace query {

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

    RelOpFilter(const Operator& op, const db::Column* column, const std::string& value)
      :Filter(1),op_(op),column_(column),value_(value) {
    }

    RelOpFilter(const RelOpFilter& other) = delete;

    Operator op() const { return op_; }
    const char* opstr() const { return opstr_[op_]; }
    const db::Column* column() const { return column_; }
    const std::string& value() const { return value_; }

    void Accept(FilterVisitor& visitor) const;

  private:
    Operator op_;
    const char* opstr_[6] = {"==", "!=", "<", "<=", ">", ">="};
    const db::Column* column_;
    const std::string value_;
};

class InFilter: public Filter {
  public:
    InFilter(const db::Column* column, const std::vector<std::string>& values)
      :Filter(4),column_(column),values_(values) {}

    InFilter(const InFilter& other) = delete;

    const db::Column* column() const { return column_; }
    const std::vector<std::string>& values() const { return values_; }

    void Accept(FilterVisitor& visitor) const;

  private:
    const db::Column* column_;
    const std::vector<std::string> values_;
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

class NotFilter: public Filter {
  public:
    NotFilter(Filter* filter):Filter(filter->precedence()),filter_(filter) {}
    NotFilter(const NotFilter& other) = delete;

    ~NotFilter() {
      delete filter_;
    }

    Filter* filter() const { return filter_; }

    void Accept(FilterVisitor& visitor) const;

  private:
    Filter* filter_;
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
    virtual void Visit(const NotFilter* filter) = 0;
    virtual void Visit(const EmptyFilter* filter) = 0;
};

class FilterFactory {
  public:
    Filter* Create(const util::Config& config, const db::Table& table);
};

}}

#endif // VIYA_QUERY_FILTER_H_
