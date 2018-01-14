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

#ifndef VIYA_CLUSTER_DOWNLOADER_H_
#define VIYA_CLUSTER_DOWNLOADER_H_

#include <string>

namespace viya {
namespace util {
class Config;
}
} // namespace viya

namespace viya {
namespace cluster {

class DownloaderBase {
public:
  virtual ~DownloaderBase() = default;
  virtual std::string Download(const std::string &path) const = 0;
};

class S3Downloader : public DownloaderBase {
public:
  std::string Download(const std::string &path) const;
};

class FSDownloader : public DownloaderBase {
public:
  std::string Download(const std::string &path) const;
};

class Downloader : public DownloaderBase {
public:
  static Downloader &Instance();
  std::string Download(const std::string &path) const;
  static std::string Fetch(const std::string &path);

private:
  const S3Downloader s3_downloader_;
  const FSDownloader fs_downloader_;
};

} // namespace cluster
} // namespace viya

#endif // VIYA_CLUSTER_DOWNLOADER_H_
