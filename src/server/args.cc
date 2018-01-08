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

#include <iostream>
#include <limits.h>
#include <stdlib.h>
#include <fstream>
#include <stdexcept>
#include <thread>
#include <glog/logging.h>
#include <boost/algorithm/string.hpp>
#include "db/defs.h"
#include "util/config.h"
#include "server/args.h"

namespace viya {
namespace server {

namespace util = viya::util;

std::string CmdlineArgs::Help() {
  std::stringstream ss;
  ss<<"ViyaDB "<<VIYA_VERSION<<std::endl
    <<"Usage: viyad [-h] [-p port] [-c cpu_list]"<<std::endl
    <<"       config-file";
  return ss.str();
}

util::Config CmdlineArgs::Parse(std::vector<std::string> args) {
  util::Config args_config;
  std::string config_file;
  for (size_t i = 1; i < args.size(); ++i) {
    if (args[i] == "-h" || args[i] == "--help") {
      std::cout<<Help()<<std::endl;
      exit(0);
    }

    if ((args[i] == "-p" || args[i] == "--port") && i+1 < args.size()) {
      args_config.set_num("http_port", std::stoi(args[++i]));
    }
    else if ((args[i] == "-c" || args[i] == "--cpu") && i+1 < args.size()) {
      std::vector<std::string> vals;
      boost::split(vals, args[++i], boost::is_any_of(","));
      std::vector<long> cpu_list;
      for (auto& v : vals) {
        cpu_list.push_back(std::stoi(v));
      }
      args_config.set_numlist("cpu_list", cpu_list);
    }
    else {
      config_file = args[i];
    }
  }
  if (config_file.empty()) {
    std::cerr<<Help()<<std::endl;
    exit(-1);
  }
  util::Config config = Defaults();
  config.MergeFrom(OpenConfig(config_file));
  config.MergeFrom(args_config);
  return config;
}

util::Config CmdlineArgs::Defaults() {
  util::Config config;
  config.set_num("http_port", 5000);
  config.set_num("query_threads", 1);
  config.set_boolean("supervise", false);
  config.set_str("state_dir", "/var/lib/viyadb");

  size_t available_cpus = std::thread::hardware_concurrency();
  std::vector<long> cpu_list(available_cpus);
  for (size_t cpu = 0; cpu < available_cpus; ++cpu) {
    cpu_list[cpu] = cpu;
  }
  config.set_numlist("cpu_list", cpu_list);
  config.set_num("workers", available_cpus);

  return config;
}

util::Config CmdlineArgs::OpenConfig(const std::string& file) {
  char log_file[PATH_MAX];
  if (realpath(file.c_str(), log_file) == nullptr) {
    throw std::runtime_error("Config file not found: " + file);
  }
  LOG(INFO)<<"Opening config file: "<<log_file;

  std::ifstream fs(log_file);
  if (!fs.good()) {
    throw std::runtime_error("Can't access config file: " + std::string(log_file));
  }
  std::stringstream buf;
  buf<<fs.rdbuf();
  util::Config config(buf.str().c_str());
  return config;
}

}}

