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
#include "util/latch.h"
#include "cluster/controller.h"
#include "cluster/loader.h"

namespace viya {
namespace cluster {

namespace fs = boost::filesystem;
namespace bi = boost::iostreams;

Loader::Loader(const Controller& controller, const std::string& load_prefix):
  controller_(controller),
  load_prefix_(load_prefix),
  load_pool_(4) {

  InitPartitionFilters();
}

void Loader::InitPartitionFilters() {
  for (auto& batches_it : controller_.indexers_batches()) {
    for (auto& tables_it : batches_it.second->tables_info()) {
      auto& table_name = tables_it.first;
      auto& table_info = tables_it.second;
      auto& table_plan = controller_.tables_plans().at(table_name);
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

void Loader::LoadFiles(const std::string& path, const std::string& table_name,
                       const std::string& worker_id) {
  auto tmpfile = ExtractFiles(path);
  load_pool_.enqueue([&] {
    LoadFile(tmpfile.string(), worker_id, table_name);
  });
}

void Loader::LoadFile(const std::string& file, const std::string& table_name,
                      const std::string& worker_id) {
  util::ScopeGuard cleanup = [&file]() {
    fs::remove(file);
  };
  json request {
    { "table", table_name },
    { "format", "tsv" },
    { "type", "file" },
    { "file", file }
  };
  SendRequest(worker_id, request);
}

void Loader::LoadFilesToAll(const std::string& path, const std::string& table_name) {
  auto tmpfile = ExtractFiles(path);
  LoadFileToAll(tmpfile.string(), table_name);
}

void Loader::LoadFileToAll(const std::string& file, const std::string& table_name) {
  util::ScopeGuard cleanup = [&file]() {
    fs::remove(file);
  };

  int fd = open(file.c_str(), O_RDONLY);
  if (fd == -1) {
    throw std::runtime_error("File not accessible: " + file);
  }
  util::ScopeGuard close_fd = [fd]() {
    close(fd);
  };

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

  auto& workers_configs = controller_.workers_configs();
  util::CountDownLatch latch(workers_configs.size());

  for (auto& worker_it : controller_.workers_configs()) {
    auto& worker_id = worker_it.first;
    json request {
      { "table", table_name },
      { "format", "tsv" },
      { "type", "file" },
      { "file", file },
      { "partition_filter", partition_filters_.at(table_name).at(worker_id) }
    };
    load_pool_.enqueue([&] {
      SendRequest(worker_id, request);
      latch.CountDown();
    });
  }

  // Wait for all workers to load the data, then cleanup
  latch.Wait();
}

std::string Loader::GetLoadUrl(const std::string& worker_id) {
  auto& worker_config = controller_.workers_configs().at(worker_id);

  return "http://" + worker_config.str("hostname")
         + ":" + std::to_string(worker_config.num("http_port")) + "/load";
}

void Loader::SendRequest(const std::string& worker_id, const json& request) {
  std::string url = GetLoadUrl(worker_id);
  auto r = cpr::Post(
             cpr::Url { url },
             cpr::Body { request.dump() },
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

fs::path Loader::ExtractFiles(const std::string& path) {
  auto tmpfile = fs::canonical(fs::unique_path());
  std::vector<fs::path> files;
  ListFiles(path, std::vector<std::string> {".gz", ".tsv", ".csv"}, files);

  for (auto& file : files) {
    std::ifstream infile(fs::canonical(file).string(), std::ios_base::in | std::ios_base::binary);
    bi::filtering_streambuf<bi::input> in;
    if (file.extension() == ".gz") {
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

void Loader::ListFiles(const std::string& path,
                       const std::vector<std::string>& exts,
                       std::vector<fs::path>& files) {
  if (!fs::exists(path) || !fs::is_directory(path)) {
    return;
  }
  fs::recursive_directory_iterator it(path);
  fs::recursive_directory_iterator endit;
  while(it != endit) {
    if (fs::is_regular_file(*it) && std::find(exts.begin(), exts.end(), it->path().extension()) != exts.end()) {
      files.push_back(it->path());
    }
    ++it;
  }
}

}}
