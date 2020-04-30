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
// @author bol (bol@pinterest.com)
//

#include "common/availability_zone.h"

#include <folly/ScopeGuard.h>
#include <glog/logging.h>

#include <cstdio>
#include <set>
#include <string>

namespace {

bool isAllowedAz(const std::string& az) {
  static const std::set<std::string> allowed_azs = {
    "us-east-1a",
    "us-east-1c",
    "us-east-1d",
    "us-east-1e",
    "eu-west-1a",
    "eu-west-1b",
  };

  return allowed_azs.find(az) != allowed_azs.end();
}

std::string getAvailabilityZoneImpl() {
  static const std::string command =
    "curl --silent --max-time 10 --connect-timeout 5 "
    "http://169.254.169.254/latest/meta-data/placement/availability-zone";

  auto f = popen(command.c_str(), "re");
  if (f == nullptr) {
    LOG(ERROR) << "Failed to popen() with errno: " << errno;
    return "";
  }

  SCOPE_EXIT {
    pclose(f);
  };

  char buf[128];
  auto len = fread(buf, 1, sizeof(buf), f);
  if (ferror(f)) {
    LOG(ERROR) << "fread() failed with errno: " << errno;
    return "";
  }

  std::string az(buf, len);

  if (isAllowedAz(az)) {
    return az;
  }

  LOG(ERROR) << "Got invalid az: " << az;
  return "";
}

}  // namespace

namespace common {

const std::string& getAvailabilityZone() {
  static const std::string local_az = getAvailabilityZoneImpl();
  return local_az;
}

}  // namespace common
