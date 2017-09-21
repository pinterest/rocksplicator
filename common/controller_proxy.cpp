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
// @author shu (shu@pinterest.com)
//

#include "common/controller_proxy.h"

#include <algorithm>
#include <cstdio>
#include <string>

#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "common/availability_zone.h"
#include "common/network_util.h"

DEFINE_string(controller_http_url, "https://controllerhttp.pinadmin.com/",
              "The url for talking to controller service");
DEFINE_string(cluster_namespace, "", "The cluster namespace");
DEFINE_string(cluster_name, "", "The cluster name");

namespace common {

std::string construct_controller_curl_cmd (
        const std::string& controller_http_curl,
        const std::string& cluster_namespace,
        const std::string& cluster_name,
        const std::string& ip_string, const uint16_t port,
        const std::string& az_string) {
  std::string mutate_ip_string = ip_string;
  std::replace(mutate_ip_string.begin(), mutate_ip_string.end(), '.', '-');
  std::string full_host_string = folly::stringPrintf(
          "%s-%s-%s", mutate_ip_string.c_str(), std::to_string(port).c_str(),
          az_string.c_str());

  std::string full_curl_cmd =folly::stringPrintf(
          "curl -X POST '%sv1/clusters/register/%s/%s?host=%s'",
          controller_http_curl.c_str(), cluster_namespace.c_str(),
          cluster_name.c_str(), full_host_string.c_str());
  return full_curl_cmd;
}

bool parse_controller_result(const std::string& curl_response) {
  // A good response is a string as "{"data":true}".
  bool is_good_response = curl_response == "{\"data\":true}";
  if (!is_good_response) {
    LOG(ERROR) << "Failed to register host: " << curl_response;
  }
  return is_good_response;
}


bool RegisterHostToController(uint16_t port) {
  if (FLAGS_controller_http_url.empty() || FLAGS_cluster_name.empty() ||
          FLAGS_cluster_namespace.empty()) {
    LOG(INFO) << "controller_http_url or cluster namespace or cluster name is"
                 " not specified, skip registration to controller.";
    return true;
  }
  std::string az_string = common::getAvailabilityZone();
  std::string ip_string = common::getLocalIPAddress();
  std::string full_curl_cmd = construct_controller_curl_cmd(
          FLAGS_controller_http_url, FLAGS_cluster_namespace,
          FLAGS_cluster_name, ip_string, port, az_string);

  auto f = popen(full_curl_cmd.c_str(), "re");
  if (f == nullptr) {
    LOG(ERROR) << "Failed to popen() with errno: " << errno;
    return false;
  }

  SCOPE_EXIT {
    pclose(f);
  };

  char buf[128];
  auto len = fread(buf, 1, sizeof(buf), f);
  if (ferror(f)) {
    LOG(ERROR) << "fread() failed with errno: " << errno;
    return false;
  }

  std::string curl_response(buf, len);

  return parse_controller_result(curl_response);
}

}  // namespace common