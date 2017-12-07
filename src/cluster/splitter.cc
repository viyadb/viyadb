#include <boost/filesystem.hpp>
#include <boost/iostreams/filtering_streambuf.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/copy.hpp>
#include <fstream>
#include "cluster/splitter.h"

namespace viya {
namespace cluster {

namespace fs = boost::filesystem;
namespace bi = boost::iostreams;

void Splitter::LoadFolder(const std::string& root, const std::string& table_name,
                          const std::string& worker_id) const {
  std::vector<fs::path> files;
  ListFiles(root, std::vector<std::string> {".gz", ".tsv", ".csv"}, files);
  std::string watch_dir = load_prefix_ + "/watch/" + worker_id + "/" + table_name;

  for (auto& path : files) {
    std::ifstream infile(fs::canonical(path).string(), std::ios_base::in | std::ios_base::binary);
    bi::filtering_streambuf<bi::input> in;
    if (path.extension() == ".gz") {
      in.push(bi::gzip_decompressor());
    }
    in.push(infile);

    auto tmpfile = fs::canonical(fs::unique_path());
    std::ofstream outfile(tmpfile.string(), std::ios_base::out | std::ios_base::binary);
    bi::filtering_streambuf<bi::output> out;
    out.push(outfile);
    bi::copy(in, out);

    // Move temporary file atomically to the target folder:
    fs::path target_file(watch_dir + "/" + tmpfile.stem().string() + ".tsv");
    fs::rename(tmpfile, target_file);
  }
}

void Splitter::ListFiles(const std::string& root,
                         const std::vector<std::string>& exts,
                         std::vector<fs::path>& files) const {

  if (!fs::exists(root) || !fs::is_directory(root)) {
    return;
  }
  fs::recursive_directory_iterator it(root);
  fs::recursive_directory_iterator endit;
  while(it != endit) {
    if (fs::is_regular_file(*it) && std::find(exts.begin(), exts.end(), it->path().extension()) != exts.end()) {
      files.push_back(it->path());
    }
    ++it;
  }
}

}}
