#include <glog/logging.h>
#include <boost/filesystem.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/copy.hpp>
#include <fstream>
#include <json.hpp>
#include <cpr/cpr.h>
#include "util/scope_guard.h"
#include "cluster/controller.h"
#include "cluster/loader.h"

namespace viya {
namespace cluster {

using json = nlohmann::json;

namespace fs = boost::filesystem;
namespace bi = boost::iostreams;

void Loader::LoadFolder(const std::string& root, const std::string& table_name,
                          const std::string& worker_id) const {
  std::vector<fs::path> files;
  ListFiles(root, std::vector<std::string> {".gz", ".tsv", ".csv"}, files);

  auto tmpfile = fs::canonical(fs::unique_path());
  util::ScopeGuard delete_tmpfile = [&tmpfile]() {
    fs::remove(tmpfile);
  };

  for (auto& path : files) {
    std::ifstream infile(fs::canonical(path).string(), std::ios_base::in | std::ios_base::binary);
    bi::filtering_streambuf<bi::input> in;
    if (path.extension() == ".gz") {
      in.push(bi::gzip_decompressor());
    }
    in.push(infile);

    std::ofstream outfile(tmpfile.string(), std::ios_base::out | std::ios_base::binary | std::ios_base::app);
    bi::filtering_streambuf<bi::output> out;
    out.push(outfile);
    bi::copy(in, out);
  }

  LoadFile(tmpfile.string(), table_name, worker_id);
}

void Loader::ListFiles(const std::string& root,
                       const std::vector<std::string>& exts,
                       std::vector<fs::path>& files) const {

  if (!fs::exists(root) || !fs::is_directory(root)) {
    return;
  }
  fs::recursive_directory_iterator it(root);
  fs::recursive_directory_iterator endit;
  while(it != endit) {
    if (fs::is_regular_file(*it) && std::find(exts.begin(), exts.end(), it->path().extension()) != exts.end()) {
      files.push_back(it->path());
    }
    ++it;
  }
}

void Loader::LoadFile(const std::string& file, const std::string& table_name,
                      const std::string& worker_id) const {

  auto& worker_config = controller_.workers_configs().at(worker_id);

  std::string url = "http://" + worker_config.str("hostname") + ":"
    + std::to_string(worker_config.num("http_port")) + "/load";

  json load_desc {
    {"table", table_name},
    {"format", "tsv"},
    {"type", "file"},
    {"file", file}
  };

  auto r = cpr::Post(
    cpr::Url { url },
    cpr::Body { load_desc.dump() },
    cpr::Header {{ "Content-Type", "application/json" }},
    cpr::Timeout { 120000L }
  );
  if (r.status_code != 200) {
    if (r.status_code == 0) {
      throw std::runtime_error("Can't contact worker at: " + url + " (host is unreachable)");
    }
    throw std::runtime_error("Can't create table in worker (" + r.text + ")");
  }
}

}}
