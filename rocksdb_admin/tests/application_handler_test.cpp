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


#include <ifaddrs.h>
#include <string>
#include <fstream>

#include "boost/filesystem.hpp"
#include "folly/String.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

#define protected public
#include "rocksdb_admin/admin_handler.h"
#undef protected



using admin::AdminHandler;
using std::string;
using std::ofstream;
using std::to_string;

DECLARE_string(shard_config_path);
DECLARE_string(rocksdb_dir);

const char* config_layout =
  "{"
  "  \"a\": {"
  "  \"num_shards\": 1,"
  "  \"%s:9090:\": [\"00000:M\"]"
  "   },"
  "  \"b\": {"
  "  \"num_shards\": 1,"
  "  \"%s:9090:\": [\"00000:M\"]"
  "   }"
  "}";

std::string getLocalIPAddress() {
  ifaddrs* ips;
  CHECK_EQ(::getifaddrs(&ips), 0);
  ifaddrs* ips_tmp = ips;
  std::string local_ip;
  const std::string interface = "eth0";
  while (ips_tmp) {
    if (interface == ips_tmp->ifa_name) {
      if (ips_tmp->ifa_addr->sa_family == AF_INET) {
        local_ip = folly::IPAddressV4(
            (reinterpret_cast<sockaddr_in*>(ips_tmp->ifa_addr))
            ->sin_addr).str();
        break;
      }
    }
    ips_tmp = ips_tmp->ifa_next;
  }
  freeifaddrs(ips);
  return local_ip;
}

void writeConfigToFile(const string& fileName, const char* config) {
  ofstream out(fileName);
  std::string ip = getLocalIPAddress();
  out << folly::stringPrintf(config, ip.data(), ip.data());
  out.close();
}

string getConfigFileName() {
  unsigned int seed = time(nullptr);
  string filename = "admin_config/aure_routing_config_" +
    to_string(rand_r(&seed));
  return filename;
}

string setupConfigs() {
  string config_file_name = getConfigFileName();
  writeConfigToFile(config_file_name, config_layout);
  return config_file_name;
}


rocksdb::Options GetRocksdbOptions(const std::string& seg) {
  rocksdb::Options options;
  options.create_if_missing = true;
  if (seg == "a") {
    options.max_open_files = 20;
  } else {
    options.max_open_files = 100;
  }
  return options;
}

TEST(AdminHandler, Basics) {
  boost::filesystem::create_directories("admin_config");
  FLAGS_shard_config_path = setupConfigs();
  FLAGS_rocksdb_dir = "admin_config/";
  AdminHandler adminHandler(nullptr, GetRocksdbOptions);
  auto dba = adminHandler.getDB("a00000", nullptr);
  auto dbb = adminHandler.getDB("b00000", nullptr);

  EXPECT_NE(dba, nullptr);
  auto optiona = dba->rocksdb()->GetOptions();
  auto optionb = dbb->rocksdb()->GetOptions();

  EXPECT_EQ(optiona.max_open_files, 20);
  EXPECT_EQ(optionb.max_open_files, 100);

  boost::filesystem::remove_all("admin_config");
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
