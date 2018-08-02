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

// Implementation of DeployInfo.

#include "common/deploy_info.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <fstream>
#include <streambuf>
#include <string>

DEFINE_string(build_revision_file, "/mnt/realpin/teletraan/build_revision",
              "File containing the build revision for the deployed build.");

using std::ifstream;
using std::string;

namespace common {

void DeployInfo::LoadRevision(const std::string& build_revision_file_path) {
  ifstream fin(build_revision_file_path);
  if (!fin) {
    LOG(WARNING) << "Could not load build info from "
                 << build_revision_file_path;
    build_revision_ = "";
  } else {
    build_revision_ = string((std::istreambuf_iterator<char>(fin)),
                             std::istreambuf_iterator<char>());
    fin.close();
  }
}

DeployInfo::DeployInfo() {
  LoadRevision(FLAGS_build_revision_file);
}

DeployInfo::DeployInfo(const std::string& build_revision_file_path) {
  LoadRevision(build_revision_file_path);
}

string DeployInfo::build_revision() const { return build_revision_; }

}  // namespace common
