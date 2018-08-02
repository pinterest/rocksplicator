/// Copyright 2018 Pinterest Inc.
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

/**
 * This class abstracts deploy related information needed for deploy validation
 * such as retrieving build revision.
 */

#pragma once

#include <string>

namespace common {

class DeployInfo {
 public:
  DeployInfo();
  explicit DeployInfo(const std::string& build_revision_file_path);

  std::string build_revision() const;

 private:
  std::string build_revision_;
  void LoadRevision(const std::string& build_revision_file_path);
};

}  // namespace common
