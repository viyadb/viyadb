#ifndef VIYA_CLUSTER_SPLITTER_H_
#define VIYA_CLUSTER_SPLITTER_H_

#include <boost/filesystem.hpp>
#include <vector>

namespace viya {
namespace cluster {

namespace fs = boost::filesystem;

class Controller;

class Splitter {
  public:
    Splitter(const std::string& load_prefix):load_prefix_(load_prefix) {}

    void LoadFolder(const std::string& root, const std::string& table_name,
                    const std::string& worker_id) const;

  private:
    void ListFiles(const std::string& root,
                   const std::vector<std::string>& exts, std::vector<fs::path>& files) const;

  private:
    const std::string load_prefix_;
};

}}

#endif // VIYA_CLUSTER_SPLITTER_H_
