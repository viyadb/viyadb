#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <boost/filesystem.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/copy.hpp>
#include <fstream>
#include <cpr/cpr.h>
#include "util/scope_guard.h"
#include "cluster/controller.h"
#include "cluster/loader.h"

namespace viya {
namespace cluster {

namespace fs = boost::filesystem;
namespace bi = boost::iostreams;

Loader::Loader(const Controller& controller, const std::string& load_prefix):
  controller_(controller),
  load_prefix_(load_prefix) {

  for (auto& batches_it : controller_.indexers_batches()) {
    for (auto& tables_it : batches_it.second->tables_info()) {
      auto& table_name = tables_it.first;
      auto& table_info = tables_it.second;
      auto& table_plan = controller.tables_plans().at(table_name);
      auto& table_filters = partition_filters_.emplace(
        table_name, std::unordered_map<std::string, json> {}).first->second;

      for (auto& it : table_plan.workers_partitions()) {
        auto& worker_id = it.first;
        auto& worker_partition = it.second;

        uint32_t value = 0;
        std::vector<uint32_t> values;
        for (auto partition : table_info.partitioning()) {
          if (partition != -1 && (uint32_t)partition == worker_partition) {
            values.push_back(value);
          }
          ++value;
        }

        auto& partition_filter = table_filters.emplace(worker_id, json {}).first->second;
        partition_filter["total_partitions"] = table_info.total_partitions();
        partition_filter["columns"] = table_info.partition_columns();
        partition_filter["values"] = values;
      }
    }
  }
}

void Loader::LoadFolderToWorker(const std::string& root, const std::string& table_name,
                                const std::string& worker_id) const {
  auto tmpfile = ExtractFiles(root);
  util::ScopeGuard delete_tmpfile = [&tmpfile]() {
    fs::remove(tmpfile);
  };

  LoadFileToWorker(tmpfile.string(), table_name, worker_id);
}

void Loader::LoadFileToWorker(const std::string& file, const std::string& table_name,
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
      LOG(ERROR)<<"Can't contact worker at: "<<url<<" (host is unreachable)";
    } else {
      LOG(ERROR)<<"Can't load data into worker ("<<r.text<<")";
    }
  }
}

void Loader::LoadFolderToAll(const std::string& root, const std::string& table_name) const {
  auto tmpfile = ExtractFiles(root);
  util::ScopeGuard delete_tmpfile = [&tmpfile]() {
    fs::remove(tmpfile);
  };

  LoadFileToAll(tmpfile.string(), table_name);
}

void Loader::LoadFileToAll(const std::string& file, const std::string& table_name) const {
  int fd = open(file.c_str(), O_RDONLY);
  if (fd == -1) {
    throw std::runtime_error("File not accessible: " + file);
  }
  util::ScopeGuard close_fd = [fd]() { close(fd); };

  struct stat fs;
  if (fstat(fd, &fs) == -1) {
    throw std::runtime_error("Can't get file status: " + file);
  }
  size_t size = fs.st_size;
  char* addr = static_cast<char*>(mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0));
  if (addr == MAP_FAILED) {
    throw std::runtime_error("Can't mmap() on file: " + file);
  }
  util::ScopeGuard unmap_addr = [addr, size, &file]() {
    if (munmap(addr, size) == -1) {
      LOG(WARNING)<<"Can't munmap() input file: "<<file;
    } 
  };

  if (madvise(addr, size, MADV_WILLNEED | MADV_SEQUENTIAL) == -1) {
    LOG(WARNING)<<"Can't madvise() on file: "<<file;
  }

  std::vector<cpr::AsyncResponse> responses;

  for (auto& worker_it : controller_.workers_configs()) {
    auto& worker_id = worker_it.first;
    auto& worker_config = worker_it.second;

    std::string url = "http://" + worker_config.str("hostname") + ":"
      + std::to_string(worker_config.num("http_port")) + "/load";

    json load_desc {
      {"table", table_name},
      {"format", "tsv"},
      {"type", "file"},
      {"file", file},
      {"partition_filter", partition_filters_.at(table_name).at(worker_id)}
    };

    responses.emplace_back(cpr::PostAsync(
      cpr::Url { url },
      cpr::Body { load_desc.dump() },
      cpr::Header {{ "Content-Type", "application/json" }},
      cpr::Timeout { 120000L }
    ));
  }

  for (auto& future : responses) {
    auto r = future.get();
    if (r.status_code != 200) {
      if (r.status_code == 0) {
        LOG(ERROR)<<"Can't contact worker at: "<<r.url<<" (host is unreachable)";
      } else {
        LOG(ERROR)<<"Can't load data into worker ("<<r.text<<")";
      }
    }
  }
}

fs::path Loader::ExtractFiles(const std::string& root) const {
  auto tmpfile = fs::canonical(fs::unique_path());
  std::vector<fs::path> files;
  ListFiles(root, std::vector<std::string> {".gz", ".tsv", ".csv"}, files);

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
  return tmpfile;
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

}}
