#include <algorithm>
#include <glog/logging.h>
#include "db/database.h"
#include "db/table.h"
#include "server/http/service.h"
#include "server/http/output.h"
#include "util/config.h"

namespace viya {
namespace server {

namespace util = viya::util;

Http::Http(const util::Config& config, db::Database& database):database_(database) {
  server_.config.port = port_ = config.num("http_port");
  server_.config.reuse_address = true;
}

void Http::SendError(ResponsePtr response, const std::string& error) {
  LOG(ERROR)<<error;
  *response<<"HTTP/1.1 400 Bad Request\r\nContent-Length: "<<error.size()<<"\r\n\r\n"<<error;
}

void Http::Start() {
  server_.resource["^/tables$"]["POST"] = [&](ResponsePtr response, RequestPtr request) {
    database_.write_pool().enqueue([=] {
      try {
        util::Config table_conf(request->content.string());
        database_.CreateTable(table_conf);
        *response<<"HTTP/1.1 201 OK\r\nContent-Length: 0\r\n\r\n";
      } catch (std::exception& e) {
        SendError(response, "Error creating table: " + std::string(e.what()));
      }
    });
  };

  server_.resource["^/tables/([^/]+)/meta$"]["GET"] = [&](ResponsePtr response, RequestPtr request) {
    database_.read_pool().enqueue([=] {
      try {
        auto table = database_.GetTable(request->path_match[1]);
        std::string meta;
        table->PrintMetadata(meta);
        *response<<"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
          <<meta.length()<<"\r\n\r\n" <<meta;
      } catch (std::exception& e) {
        SendError(response, "Error querying table metadata: " + std::string(e.what()));
      }
    });
  };

  server_.resource["^/database/meta$"]["GET"] = [&](ResponsePtr response, RequestPtr request __attribute__((unused))) {
    database_.read_pool().enqueue([=] {
      try {
        std::string meta;
        database_.PrintMetadata(meta);
        *response<<"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
          <<meta.length()<<"\r\n\r\n" <<meta;
      } catch (std::exception& e) {
        SendError(response, "Error querying database metadata: " + std::string(e.what()));
      }
    });
  };

  server_.resource["^/load$"]["POST"] = [&](ResponsePtr response, RequestPtr request) {
    database_.write_pool().enqueue([=] {
      try {
        util::Config load_conf(request->content.string());
        database_.Load(load_conf);
        *response<<"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
      } catch (std::exception& e) {
        SendError(response, "Error loading data: " + std::string(e.what()));
      }
    });
  };

  server_.resource["^/query(\\?.*)?$"]["POST"] = [&](ResponsePtr response, RequestPtr request) {
    database_.read_pool().enqueue([=] {
      try {
        util::Config query_conf(request->content.string());
        auto params = request->parse_query_string();
        if (params.find("header") != params.end()) {
          query_conf.set_boolean("header", true);
        }
        ChunkedTsvOutput output(*response);
        database_.Query(query_conf, output);
      } catch (std::exception& e) {
        SendError(response, "Error querying table: " + std::string(e.what()));
      }
    });
  };

  LOG(INFO)<<"Started HTTP service on port "<<std::to_string(server_.config.port)<<std::endl;
  server_.start();
}

}}

