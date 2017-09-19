#ifndef VIYA_SERVER_HTTP_SERVICE_H_
#define VIYA_SERVER_HTTP_SERVICE_H_

#include <server_http.hpp>
#include "util/config.h"
#include "db/database.h"

namespace viya {
namespace server {

namespace util = viya::util;
namespace db = viya::db;

typedef SimpleWeb::Server<SimpleWeb::HTTP> HttpServer;
typedef std::shared_ptr<HttpServer::Request> RequestPtr;
typedef std::shared_ptr<HttpServer::Response> ResponsePtr;

class Http {
  public:
    Http(const util::Config& config, db::Database& database);

    uint16_t port() const { return port_; }

    void Start();

  private:
    void SendError(ResponsePtr response, const std::string& error);

  private:
    db::Database& database_;
    HttpServer server_;
    uint16_t port_;
};

}}

#endif // VIYA_SERVER_HTTP_SERVICE_H_
