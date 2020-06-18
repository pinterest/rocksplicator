/// Copyright 2019 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

#include <fstream>
#include <folly/ScopeGuard.h>
#include <glog/logging.h>

#include "common/kafka/kafka_config.h"

namespace kafka {

/**
 * Read Java kafka client configuration file
 */
bool KafkaConfig::read_conf_file(
  const std::string& conf_file,
  ConfigMap* configMap) {
  std::ifstream input_stream(conf_file.c_str());

  if (!input_stream || !input_stream.is_open()) {
    LOG(ERROR) << ": " << conf_file << ": could not open file" << std::endl;
    return false;
  }

  LOG(INFO) << ": " << conf_file << ": read config file" << std::endl;

  std::string line;
  int linenr = 0;

  SCOPE_EXIT {
    input_stream.close();
  };

  while (std::getline(input_stream, line)) {
    linenr++;

    // Ignore comments and empty lines
    if (line.length() == 0 || line[0] == '#')
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

    (*configMap)[key] = std::move(val);
  }
  return true;
}
} // namespace kafka
