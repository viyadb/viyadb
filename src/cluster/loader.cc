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
#include "cluster/batch_info.h"

namespace viya {
namespace cluster {

namespace fs = boost::filesystem;
namespace bi = boost::iostreams;

Loader::Loader(const Controller& controller, const std::string& load_prefix):
  controller_(controller),
  load_prefix_(load_prefix),
  load_pool_(4) {
}

json Loader::GetPartitionFilter(const std::string& table_name, const std::string& worker_id) {
  auto& partitioning = controller_.tables_partitioning().at(table_name);
  auto& table_plan = controller_.tables_plans().at(table_name);
  auto worker_partition = table_plan.workers_partitions().at(worker_id);

  std::vector<uint32_t> values;
  auto& value_to_partition = partitioning.mapping();
  for (uint32_t value = 0; value < value_to_partition.size(); ++value) {
    if (value_to_partition[value] == worker_partition) {
      values.push_back(value);
    }
  }
  return std::move(json {
    { "total_partitions", partitioning.total() },
    { "columns", partitioning.columns() },
    { "values", values }
  });
}

void Loader::LoadFiles(const std::string& path, const std::string& table_name,
                       const TableInfo& table_info, const std::string& worker_id) {
  auto tmpfile = ExtractFiles(path);
  load_pool_.enqueue([&] {
    LoadFile(tmpfile.string(), table_name, table_info, worker_id);
  });
}

void Loader::LoadFile(const std::string& file, const std::string& table_name,
                      const TableInfo& table_info, const std::string& worker_id) {
  util::ScopeGuard cleanup = [&file]() {
    fs::remove(file);
  };
  json request {
    { "table", table_name },
    { "columns", table_info.columns() },
    { "format", "tsv" },
    { "type", "file" },
    { "file", file }
  };
  SendRequest(worker_id, request);
}

void Loader::LoadFilesToAll(const std::string& path, const std::string& table_name,
                            const TableInfo& table_info) {
  auto tmpfile = ExtractFiles(path);
  LoadFileToAll(tmpfile.string(), table_name, table_info);
}

void Loader::LoadFileToAll(const std::string& file, const std::string& table_name,
                           const TableInfo& table_info) {
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

  auto& workers_parts = controller_.tables_plans().at(table_name).workers_partitions();
  util::CountDownLatch latch(workers_parts.size());

  for (auto& it : workers_parts) {
    auto& worker_id = it.first;
    json request {
      { "table", table_name },
      { "columns", table_info.columns() },
      { "format", "tsv" },
      { "type", "file" },
      { "file", file },
      { "partition_filter", GetPartitionFilter(table_name, worker_id) }
    };
    load_pool_.enqueue([&, request] {
      SendRequest(worker_id, request);
      latch.CountDown();
    });
  }

  // Wait for all workers to load the data, then cleanup
  latch.Wait();
}

void Loader::SendRequest(const std::string& worker_id, const json& request) {
  std::string url = "http://" + worker_id + "/load";
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
      throw std::runtime_error("Can't load data into worker (" + r.text + ")");
    }
  }
}

fs::path Loader::ExtractFiles(const std::string& path) {
  auto tmpfile = fs::temp_directory_path() / fs::unique_path();
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
