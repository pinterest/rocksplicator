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

#include "common/network_util.h"

#include <ifaddrs.h>
#include "folly/SocketAddress.h"
#include <glog/logging.h>

namespace {

std::string getLocalIPAddressImpl() {
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

}  // namespace


namespace common {

const std::string& getLocalIPAddress() {
  static const std::string local_ip = getLocalIPAddressImpl();
  return local_ip;
}

std::string getNetworkAddressStr(const folly::SocketAddress& addr) noexcept {
  std::string add_str = "unknown_addr";
  try {
    add_str = addr.getAddressStr();
  } catch (const std::exception& e) {
    LOG(ERROR) << "cannot get upstream address: " << e.what();
  }
  return add_str;
}

}  // namespace common
