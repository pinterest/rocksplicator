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

#pragma once

#include <string>

#include "folly/SocketAddress.h"


namespace common {

/*
 * Get the local IP address from eth0.
 */
const std::string& getLocalIPAddress();

/**
 * Get a string representation of the provide socket address if it is IPv4 or IPv6,
 * return "uninitialized_addr" if an empty address is provided;
 * return "unknown_addr" if an error is encountered parsing the address.
 */
std::string getNetworkAddressStr(const folly::SocketAddress&) noexcept;

}  // namespace common