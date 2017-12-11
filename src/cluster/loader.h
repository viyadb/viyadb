#ifndef VIYA_CLUSTER_LOADER_H_
#define VIYA_CLUSTER_LOADER_H_

#include <boost/filesystem.hpp>
#include <vector>
#include <unordered_map>
#include <json.hpp>

namespace viya {
namespace cluster {

namespace fs = boost::filesystem;

using json = nlohmann::json;

class Controller;

class Loader {
  public:
    Loader(const Controller& controller, const std::string& load_prefix);
    Loader(const Loader& other) = delete;

    void LoadFolderToWorker(const std::string& root, const std::string& table_name,
                            const std::string& worker_id) const;

    void LoadFileToWorker(const std::string& file, const std::string& table_name,
                          const std::string& worker_id) const;

    void LoadFolderToAll(const std::string& root, const std::string& table_name) const;

    void LoadFileToAll(const std::string& file, const std::string& table_name) const;

  private:
    fs::path ExtractFiles(const std::string& root) const;

    void ListFiles(const std::string& root, const std::vector<std::string>& exts,
                   std::vector<fs::path>& files) const;

  private:
    const Controller& controller_;
    const std::string load_prefix_;
    std::unordered_map<std::string, std::unordered_map<std::string, json>> partition_filters_;
};

}}

#endif // VIYA_CLUSTER_LOADER_H_
