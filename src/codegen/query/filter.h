#ifndef VIYA_CODEGEN_QUERY_FILTER_H_
#define VIYA_CODEGEN_QUERY_FILTER_H_

#include "query/filter.h"
#include "codegen/generator.h"

namespace viya {
namespace codegen {

namespace query = viya::query;

class FilterArgsPacker: public query::FilterVisitor {
  public:
    FilterArgsPacker() {}

    void Visit(const query::RelOpFilter* filter);
    void Visit(const query::InFilter* filter);
    void Visit(const query::CompositeFilter* filter);
    void Visit(const query::NotFilter* filter);
    void Visit(const query::EmptyFilter* filter);

    const std::vector<db::AnyNum>& args() const { return args_; }

  private:
    std::vector<db::AnyNum> args_;
};

class ArgsUnpacker: public query::FilterVisitor {
  public:
    ArgsUnpacker(Code& code, const std::string& var_prefix)
      :argidx_(0),code_(code),var_prefix_(var_prefix) {}

    ArgsUnpacker(const ArgsUnpacker& other) = delete;

    void Visit(const query::RelOpFilter* filter);
    void Visit(const query::InFilter* filter);
    void Visit(const query::CompositeFilter* filter);
    void Visit(const query::NotFilter* filter);
    void Visit(const query::EmptyFilter* filter);

  private:
    void UnpackArg(const db::Column* column);

  private:
    size_t argidx_;
    Code& code_;
    const std::string var_prefix_;
};

class ValueDecoder: public db::ColumnVisitor {
  public:
    ValueDecoder(const std::string& value):value_(value) {}

    void Visit(const db::StrDimension* dimension);
    void Visit(const db::NumDimension* dimension);
    void Visit(const db::TimeDimension* dimension);
    void Visit(const db::BoolDimension* dimension);
    void Visit(const db::ValueMetric* metric);
    void Visit(const db::BitsetMetric* metric);

    const db::AnyNum& decoded_value() const { return decoded_value_; }

  private:
    const std::string& value_;
    db::AnyNum decoded_value_;
};

class ComparisonBuilder: public query::FilterVisitor {
  public:
    ComparisonBuilder(Code& code, const std::string& var_prefix = "farg")
      :argidx_(0),code_(code),var_prefix_(var_prefix),in_not_(false) {}

    void Visit(const query::RelOpFilter* filter);
    void Visit(const query::InFilter* filter);
    void Visit(const query::CompositeFilter* filter);
    void Visit(const query::NotFilter* filter);
    void Visit(const query::EmptyFilter* filter);

  protected:
    size_t argidx_;
    Code& code_;
    const std::string var_prefix_;
    bool in_not_;
};

class SegmentSkipBuilder: public ComparisonBuilder {
  public:
    SegmentSkipBuilder(Code& code):ComparisonBuilder(code) {}

    void Visit(const query::RelOpFilter* filter);
    void Visit(const query::InFilter* filter);
};

class FilterArgsUnpack: public CodeGenerator {
  public:
    FilterArgsUnpack(const query::Filter* filter, const std::string& var_prefix)
      :filter_(filter),var_prefix_(var_prefix) {}
    FilterArgsUnpack(const FilterArgsUnpack& other) = delete;

    Code GenerateCode() const;

  private:
    const query::Filter* filter_;
    const std::string var_prefix_;
};

class SegmentSkip: CodeGenerator {
  public:
    SegmentSkip(const query::Filter* filter):filter_(filter) {}
    SegmentSkip(const SegmentSkip& other) = delete;

    Code GenerateCode() const;

  private:
    const query::Filter* filter_;
};

class FilterComparison: CodeGenerator {
  public:
    FilterComparison(const query::Filter* filter, const std::string& var_prefix)
      :filter_(filter),var_prefix_(var_prefix) {}
    FilterComparison(const FilterComparison& other) = delete;

    Code GenerateCode() const;

  private:
    const query::Filter* filter_;
    const std::string var_prefix_;
};

}}

#endif // VIYA_CODEGEN_QUERY_FILTER_H_
