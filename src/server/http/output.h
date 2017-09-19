#ifndef VIYA_SERVER_HTTP_OUTPUT_H_
#define VIYA_SERVER_HTTP_OUTPUT_H_

#include <sstream>
#include "query/output.h"

namespace viya {
namespace server {

namespace query = viya::query;

class ChunkedTsvOutput: public query::RowOutput {
  public:
    ChunkedTsvOutput(std::ostream& stream, char col_sep='\t', char row_sep='\n')
      :stream_(stream),chunk_size_(16384),col_sep_(col_sep),row_sep_(row_sep) {
    }

    ChunkedTsvOutput(const ChunkedTsvOutput& other) = delete;
    ~ChunkedTsvOutput() {}

    void Start() {
      stream_<<std::hex;
      stream_<<"HTTP/1.1 200 OK\r\nContent-Type: text/tab-separated-values\r\nTransfer-Encoding: chunked\r\n\r\n";
    }

    void SendAsCol(const Row& row) {
      col_sep_ = '\n';
      Send(row);
    }

    void Send(const Row& row) {
      auto size = row.size();
      for (size_t i = 0; i < size; ++i) {
        if (i > 0) {
          buf_<<col_sep_;
        }
        buf_<<row[i];
      }
      buf_<<row_sep_;
      if (buf_.tellp() >= chunk_size_) {
        stream_<<buf_.tellp()<<crlf_;
        stream_<<buf_.str()<<crlf_;
        buf_.str("");
      }
    }

    void Flush() {
      if (buf_.tellp() > 0) {
        stream_<<buf_.tellp()<<crlf_;
        stream_<<buf_.str()<<crlf_;
      }
      stream_<<0;
      stream_<<crlf_<<crlf_;
    }

  private:
    static constexpr const char* crlf_ = "\r\n";
    std::ostream& stream_;
    long long chunk_size_;
    char col_sep_;
    char row_sep_;
    std::ostringstream buf_;
};

}}

#endif // VIYA_SERVER_HTTP_OUTPUT_H_
