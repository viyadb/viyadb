#ifndef VIYA_CLUSTER_LOADER_H_
#define VIYA_CLUSTER_LOADER_H_

#include <boost/filesystem.hpp>
#include <vector>

namespace viya {
namespace cluster {

namespace fs = boost::filesystem;

class Controller;

class Loader {
  public:
    Loader(const Controller& controller, const std::string& load_prefix)
      :controller_(controller), load_prefix_(load_prefix) {}

    void LoadFolder(const std::string& root, const std::string& table_name,
                    const std::string& worker_id) const;

  private:
    void LoadFile(const std::string& file, const std::string& table_name,
                  const std::string& worker_id) const;

    void ListFiles(const std::string& root,
                   const std::vector<std::string>& exts, std::vector<fs::path>& files) const;

  private:
    const Controller& controller_;
    const std::string load_prefix_;
};

}}

#endif // VIYA_CLUSTER_LOADER_H_
