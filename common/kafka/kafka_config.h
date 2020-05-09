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

typedef std::map<std::string, std::pair<std::string, bool>> ConfigMap;

class KafkaConfig {
public:
  static bool read_conf_file(
    const std::string &conf_file,
    const std::shared_ptr<ConfigMap> configMap,
    bool compatibility_flag);
};
} // namespace kafka
