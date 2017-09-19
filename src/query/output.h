#ifndef VIYA_QUERY_OUTPUT_H_
#define VIYA_QUERY_OUTPUT_H_

#include <vector>
#include <string>

namespace viya {
namespace query {

class RowOutput {
  public:
    using Row = std::vector<std::string>;

    RowOutput() {}
    virtual ~RowOutput() {}

    virtual void Start() {};
    virtual void Send(const Row& row) = 0;
    virtual void SendAsCol(const Row& col) = 0;
    virtual void Flush() {};
};

class MemoryRowOutput: public RowOutput {
  public:
    void Send(const Row& row) { rows_.push_back(row); }
    void SendAsCol(const Row& row) { rows_.push_back(row); }

    const std::vector<Row>& rows() const { return rows_; }

  private:
    std::vector<Row> rows_;
};

}}

#endif // VIYA_QUERY_OUTPUT_H_
