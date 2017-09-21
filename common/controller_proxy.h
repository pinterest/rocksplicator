/// Copyright 2016 Pinterest Inc.
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

//
// Proxy functions to talk to controller for cluster management functions.
//
// @author shu (shu@pinterest.com)
//

#pragma once

#include <string>

#include <gflags/gflags.h>

namespace common {

std::string construct_controller_curl_cmd(
        const std::string& controller_http_curl,
        const std::string& cluster_namespace,
        const std::string& cluster_name,
        const std::string& ip_string,
        const int port, const std::string& az_string);

// We need to specify port here because port flag is defined in realpin and
// rocksplicator, so we avoid using gflag directly.
bool registerHostToController(const int port);

}  // namespace common