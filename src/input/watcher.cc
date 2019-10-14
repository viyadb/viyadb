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

#include "input/watcher.h"
#include "db/database.h"
#include "db/table.h"
#include <algorithm>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/filesystem.hpp>
#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <stdexcept>
#include <sys/inotify.h>
#include <sys/select.h>
#include <unistd.h>

namespace viya {
namespace input {

namespace fs = boost::filesystem;

Watcher::Watcher(db::Database &db) : db_(db), running_(true) {
  fd_ = inotify_init();
  if (fd_ == -1) {
    throw std::runtime_error(std::strerror(errno));
  }
  fcntl(fd_, F_SETFL, fcntl(fd_, F_GETFL) | O_NONBLOCK);
}

void Watcher::AddWatch(const util::Config &config, db::Table *table) {
  if (!thread_.joinable()) {
    thread_ = std::thread(&Watcher::Run, this);
  }

  std::lock_guard<std::mutex> lock(mutex_);

  fs::path watch_dir(config.str("directory"));
  fs::create_directories(watch_dir);

  char *dir = realpath(fs::canonical(watch_dir).string().c_str(), nullptr);
  LOG(INFO) << "Started watching directory for new files: " << dir;

  int wd = inotify_add_watch(fd_, dir, IN_MOVED_TO | IN_EXCL_UNLINK);
  if (wd == -1) {
    free(dir);
    throw std::runtime_error(std::strerror(errno));
  }
  watches_.emplace_back(table, std::string(dir),
                        config.strlist("extensions", {".tsv"}), wd);
  free(dir);
}

void Watcher::RemoveWatch(db::Table *table) {
  std::lock_guard<std::mutex> lock(mutex_);

  watches_.erase(std::remove_if(watches_.begin(), watches_.end(),
                                [table](Watch &w) { return w.table == table; }),
                 watches_.end());
}

std::vector<std::string> Watcher::ScanFiles(Watch &watch) {
  std::vector<std::string> files;
  DIR *dp = opendir(watch.dir.c_str());
  if (dp != nullptr) {
    dirent *de;
    while ((de = readdir(dp)) != nullptr) {
      std::string file(de->d_name);
      if (std::any_of(watch.exts.begin(), watch.exts.end(), [&file](auto &ext) {
            return boost::algorithm::ends_with(file, ext);
          })) {
        files.push_back(watch.dir + "/" + file);
      }
    }
    closedir(dp);
    std::sort(files.begin(), files.end());
  }
  return files;
}

void Watcher::ProcessEvent(Watch &watch) {
  for (auto &file : ScanFiles(watch)) {
    if (watch.last_file.empty() || watch.last_file < file) {
      db_.write_pool().enqueue([=] {
        try {
          util::Config load_conf;
          load_conf.set_str("type", "file");
          load_conf.set_str("file", file);
          load_conf.set_str("format", "tsv");
          load_conf.set_str("table", watch.table->name());
          db_.Load(load_conf);
        } catch (std::exception &e) {
          LOG(ERROR) << "Error loading file " << file << ": " << e.what();
        }
      });
      watch.last_file = file;
    }
  }
}

void Watcher::Run() {
  static const size_t EVENT_SIZE = sizeof(struct inotify_event);
  static const size_t BUF_LEN = 1024 * (EVENT_SIZE + 16);
  char buffer[BUF_LEN];

  fd_set fdset;
  FD_ZERO(&fdset);
  FD_SET(fd_, &fdset);
  struct timeval timeout;
  timeout.tv_sec = 1;
  timeout.tv_usec = 0;

  while (running_) {
    int n = select(fd_ + 1, &fdset, NULL, NULL, &timeout);
    if (n == -1) {
      throw std::runtime_error(std::strerror(errno));
    }
    if (!n) {
      continue;
    }
    ssize_t length = read(fd_, buffer, BUF_LEN);
    if (length == -1) {
      throw std::runtime_error(std::strerror(errno));
    }

    ssize_t i = 0;
    while (i < length) {
      struct inotify_event *event = (struct inotify_event *)&buffer[i];

      if (event->len > 0) {
        if ((event->mask & IN_MOVED_TO) != 0 && (event->mask & IN_ISDIR) == 0) {
          std::unique_lock<std::mutex> lock(mutex_);
          auto it =
              std::find_if(watches_.begin(), watches_.end(),
                           [event](Watch &w) { return w.wd == event->wd; });
          lock.unlock();
          if (it != watches_.end()) {
            ProcessEvent(*it);
          }
        }
      }
      i += EVENT_SIZE + event->len;
    }
  }
}

Watcher::~Watcher() {
  running_ = false;

  if (fd_ != -1) {
    for (auto &watch : watches_) {
      inotify_rm_watch(fd_, watch.wd);
    }
    close(fd_);
  }

  if (thread_.joinable()) {
    thread_.join();
  }
}
} // namespace input
} // namespace viya
