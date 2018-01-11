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

#ifndef VIYA_CLUSTER_LOADER_H_
#define VIYA_CLUSTER_LOADER_H_

#include <ThreadPool/ThreadPool.h>
#include <boost/filesystem.hpp>
#include <json.hpp>
#include <unordered_map>
#include <vector>

namespace viya {
namespace cluster {

namespace fs = boost::filesystem;

using json = nlohmann::json;

class Controller;
class TableInfo;

class Loader {
public:
  Loader(const Controller &controller, const std::string &load_prefix);
  Loader(const Loader &other) = delete;

  void LoadFiles(const std::string &path, const std::string &table_name,
                 const TableInfo &table_info, const std::string &worker_id);

  void LoadFilesToAll(const std::string &path, const std::string &table_name,
                      const TableInfo &table_info);

private:
  json GetPartitionFilter(const std::string &table_name,
                          const std::string &worker_id);

  void LoadFile(const std::string &file, const std::string &table_name,
                const TableInfo &table_info, const std::string &worker_id);

  void LoadFileToAll(const std::string &file, const std::string &table_name,
                     const TableInfo &table_info);

  void SendRequest(const std::string &url, const json &request);

  fs::path ExtractFiles(const std::string &path);

  void ListFiles(const std::string &path, const std::vector<std::string> &exts,
                 std::vector<fs::path> &files);

private:
  const Controller &controller_;
  const std::string load_prefix_;
  ThreadPool load_pool_;
};

} // namespace cluster
} // namespace viya

#endif // VIYA_CLUSTER_LOADER_H_
