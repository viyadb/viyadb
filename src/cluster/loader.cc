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

#include "cluster/loader.h"
#include "cluster/batch_info.h"
#include "cluster/controller.h"
#include "util/latch.h"
#include "util/scope_guard.h"
#include <boost/filesystem.hpp>
#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <cpr/cpr.h>
#include <fcntl.h>
#include <fstream>
#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace viya {
namespace cluster {

namespace fs = boost::filesystem;
namespace bi = boost::iostreams;

void DeleteFile(const std::string &file) {
  DLOG(INFO) << "Deleting file: " << file;
  fs::remove(file);
}

Loader::Loader(const Controller &controller, const std::string &load_prefix)
    : controller_(controller), load_prefix_(load_prefix),
      load_pool_(controller.config().num("workers")) {}

util::Config Loader::GetPartitionFilter(const std::string &table_name,
                                        const std::string &worker_id) {
  auto &partitioning = controller_.tables_partitioning().at(table_name);
  auto &table_plan = controller_.tables_plans().at(table_name);
  auto worker_partition = table_plan.workers_partitions().at(worker_id);

  std::vector<uint32_t> values;
  auto &value_to_partition = partitioning.mapping();
  for (uint32_t value = 0; value < value_to_partition.size(); ++value) {
    if (value_to_partition[value] == worker_partition) {
      values.push_back(value);
    }
  }

  return std::move(util::Config(json{{"total_partitions", partitioning.total()},
                                     {"columns", partitioning.columns()},
                                     {"values", values}}));
}

void Loader::Load(const util::Config &load_desc, const std::string &worker_id) {

  auto tmpfile = ExtractFiles(load_desc.str("file"));
  util::ScopeGuard cleanup = [&tmpfile]() { DeleteFile(tmpfile); };

  util::Config tmp_desc = load_desc;
  tmp_desc.set_str("file", tmpfile);
  auto data = tmp_desc.dump();

  if (!worker_id.empty()) {
    load_pool_.enqueue([&] { SendRequest(worker_id, data); });

  } else {
    int fd = open(tmpfile.c_str(), O_RDONLY);
    if (fd == -1) {
      throw std::runtime_error("File not accessible: " + tmpfile);
    }
    util::ScopeGuard close_fd = [fd]() { close(fd); };

    struct stat fs;
    if (fstat(fd, &fs) == -1) {
      throw std::runtime_error("Can't get file status: " + tmpfile);
    }
    size_t size = fs.st_size;
    char *addr =
        static_cast<char *>(mmap(NULL, size, PROT_READ, MAP_SHARED, fd, 0));
    if (addr == MAP_FAILED) {
      throw std::runtime_error("Can't mmap() on file: " + tmpfile);
    }
    util::ScopeGuard unmap_addr = [addr, size, &tmpfile]() {
      if (munmap(addr, size) == -1) {
        LOG(WARNING) << "Can't munmap() input file: " << tmpfile;
      }
    };
    if (madvise(addr, size, MADV_WILLNEED | MADV_SEQUENTIAL) == -1) {
      LOG(WARNING) << "Can't madvise() on file: " << tmpfile;
    }

    auto table_name = load_desc.str("table");
    auto &workers_parts =
        controller_.tables_plans().at(table_name).workers_partitions();
    util::CountDownLatch latch(workers_parts.size());

    for (auto &it : workers_parts) {
      auto &worker_id = it.first;
      util::Config tmp_desc = load_desc;
      auto partition_filter =
          std::move(GetPartitionFilter(table_name, worker_id));
      tmp_desc.set_sub("partition_filter", partition_filter);
      auto data = tmp_desc.dump();

      load_pool_.enqueue([&] {
        SendRequest(worker_id, data);
        latch.CountDown();
      });
    }

    // Wait for all workers to load the data, then cleanup
    latch.Wait();
  }
}

void Loader::SendRequest(const std::string &worker_id,
                         const std::string &data) {
  std::string url = "http://" + worker_id + "/load";
  auto r = cpr::Post(cpr::Url{url}, cpr::Body{data},
                     cpr::Header{{"Content-Type", "application/json"}});
  if (r.status_code != 200 && r.status_code == 0) {
    LOG(ERROR) << "Can't contact worker at: " << url
               << " (host is unreachable)";
  }
}

std::string Loader::ExtractFiles(const std::string &path) {
  auto tmpfile = fs::temp_directory_path() / fs::unique_path();
  DLOG(INFO) << "Creating temporary file: " << tmpfile.string();

  std::vector<fs::path> files;
  ListFiles(path, std::vector<std::string>{".gz", ".tsv", ".csv"}, files);

  for (auto &file : files) {
    std::ifstream infile(fs::canonical(file).string(),
                         std::ios_base::in | std::ios_base::binary);
    bi::filtering_streambuf<bi::input> in;
    if (file.extension() == ".gz") {
      in.push(bi::gzip_decompressor());
    }
    in.push(infile);

    std::ofstream outfile(tmpfile.string(),
                          std::ios_base::out | std::ios_base::binary |
                              std::ios_base::app);
    bi::filtering_streambuf<bi::output> out;
    out.push(outfile);
    bi::copy(in, out);
  }
  return tmpfile.string();
}

void Loader::ListFiles(const std::string &path,
                       const std::vector<std::string> &exts,
                       std::vector<fs::path> &files) {
  if (!fs::exists(path) || !fs::is_directory(path)) {
    return;
  }
  fs::recursive_directory_iterator it(path);
  fs::recursive_directory_iterator endit;
  while (it != endit) {
    if (fs::is_regular_file(*it) &&
        std::find(exts.begin(), exts.end(), it->path().extension()) !=
            exts.end()) {
      files.push_back(it->path());
    }
    ++it;
  }
}

} // namespace cluster
} // namespace viya
