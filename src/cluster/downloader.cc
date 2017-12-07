#include <glog/logging.h>
#include <stdexcept>
#include <cstdlib>
#include "util/process.h"
#include "cluster/downloader.h"

namespace viya {
namespace cluster {

std::string S3Downloader::Download(const std::string& path) const {
  char tmpdir[] = "/tmp/viyadb-download.XXXXXX";
  std::string target_path(mkdtemp(tmpdir));

  // This may seem silly to call external AWS CLI binary instead of using AWS SDK,
  // but according to performance tests this command beats any AWS SDK:
  if (util::Process::Run(
      {"aws", "s3", "sync", "--only-show-errors", "--no-progress", path.c_str(), target_path.c_str()}) != 0) {
    throw std::runtime_error("Can't fetch files from S3!");
  }
  return target_path;
}

std::string FSDownloader::Download(const std::string& path) const {
  // Nothing to do
  return path;
}

std::string Downloader::Download(const std::string& path) const {
  LOG(INFO)<<"Fetching "<<path;
  if (path.rfind("s3:", 0) == 0) {
    return s3_downloader_.Download(path);
  }
  if (path.rfind("file:", 0) == 0 || path.rfind("/", 0) == 0) {
    return fs_downloader_.Download(path);
  }
  throw std::runtime_error("Can't find appropriate downloader for path: " + path);
}

Downloader& Downloader::Instance() {
  static Downloader instance;
  return instance;
}

std::string Downloader::Fetch(const std::string& path) {
  return Instance().Download(path);
}

}}
