#ifndef VIYA_CLUSTER_DOWNLOADER_H_
#define VIYA_CLUSTER_DOWNLOADER_H_

#include <string>

namespace viya { namespace util { class Config; }}

namespace viya {
namespace cluster {

class DownloaderBase {
  public:
    virtual ~DownloaderBase() = default;
    virtual std::string Download(const std::string& path) const = 0;
};

class S3Downloader: public DownloaderBase {
  public:
    std::string Download(const std::string& path) const;
};

class FSDownloader: public DownloaderBase {
  public:
    std::string Download(const std::string& path) const;
};

class Downloader: public DownloaderBase {
  public:
    static Downloader& Instance();
    std::string Download(const std::string& path) const;

  private:
    const S3Downloader s3_downloader_;
    const FSDownloader fs_downloader_;
};

}}

#endif // VIYA_CLUSTER_DOWNLOADER_H_
