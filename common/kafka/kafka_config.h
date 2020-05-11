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

#pragma once

#include <map>
#include <memory>
#include <string>

namespace kafka {

typedef std::map<std::string, std::string> ConfigMap;

class KafkaConfig {
public:
  // This method reads in properties/configuration in key=value pair format
  // into configMap.
  // The format of properties file is similar to java properties file
  // except it doesn't dereference any escape characters or special characters.
  // Empty lines must not contain any whitespace.
  // parameters:
  // conf_file: path of configuration file to read.
  // configMap: Output-only parameter, map containing all key-value pairs read
  //            from the provided config file.
  static bool read_conf_file(
    const std::string &conf_file,
    ConfigMap &configMap);
};
} // namespace kafka
