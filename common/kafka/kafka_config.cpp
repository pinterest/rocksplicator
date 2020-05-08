//
// Created by Gopal Rajpurohit on 5/8/20.
//

#include <iostream>
#include <fstream>
#include <sstream>

#include "common/kafka/kafka_config.h"
#include "glog/logging.h"

using namespace std;

namespace kafka {
/**
 * @brief Read (Java client) configuration file
 */
static bool read_conf_file(
  const std::string &conf_file,
  const std::shared_ptr<ConfigMap>& configMap) {
  std::ifstream inf(conf_file.c_str());

  if (!inf) {
    LOG(ERROR) << ": " << conf_file << ": could not open file" << std::endl;
    return false;
  }

  LOG(INFO) << ": " << conf_file << ": read config file" << std::endl;

  std::string line;
  int linenr = 0;

  while (std::getline(inf, line)) {
    linenr++;

    // Ignore comments and empty lines
    if (line[0] == '#' || line.length() == 0)
      continue;

    // Match on key=value..
    size_t d = line.find("=");
    if (d == 0 || d == std::string::npos) {
      LOG(INFO) << ": " << conf_file << ":" << linenr << ": " << line
                << ": invalid line (expect key=value): " << ::std::endl;
      return false;
    }

    std::string key = line.substr(0, d);
    std::string val = line.substr(d + 1);

    (*configMap)[key] = make_pair(val, false);
  }
  inf.close();
  return true;
}
} // namespace kafka