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

#include "gtest/gtest.h"

#include "common/controller_proxy.h"

namespace common {

std::string construct_controller_curl_cmd(
        const std::string& controller_http_curl,
        const std::string& cluster_namespace,
        const std::string& cluster_name,
        const std::string& ip_string,
        const uint16_t port, const std::string& az_string);

bool parse_controller_result(const std::string& curl_response);
}  // namespace common


TEST(ControllerProxyTest, UrlTest) {
  std::string real_curl_cmd = common::construct_controller_curl_cmd(
          "https://controllerhttp.pinadmin.com/",
          "rocksdb", "aperture-shared", "1.2.3.4", 9090, "us-east-1a");
  EXPECT_EQ(real_curl_cmd, "curl -X POST 'https://controllerhttp.pinadmin.com/v1/clusters/register/rocksdb/aperture-shared?host=1-2-3-4-9090-us-east-1a'");
}

TEST(ControllerProxyTest, CurlResponseTest) {
  EXPECT_TRUE(common::parse_controller_result("{\"data\":true}"));
  EXPECT_FALSE(common::parse_controller_result("{\"data\":false}"));
}


int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}