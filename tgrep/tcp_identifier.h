/// Copyright 2017 Pinterest Inc.
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

#pragma once

#include <cstdint>

#include <netinet/in.h>

namespace tgrep {

struct TcpIdentifier {
  TcpIdentifier(uint16_t port_src_arg,
                uint16_t port_dest_arg,
                struct in_addr ip_src_arg,
                struct in_addr ip_dest_arg) :
      port_src(port_src_arg),
      port_dest(port_dest_arg),
      ip_src(ip_src_arg),
      ip_dest(ip_dest_arg) {
  }

  TcpIdentifier getConnectionIdentifier() const;

  bool operator < (const TcpIdentifier& tcp) const;

  const uint16_t port_src;
  const uint16_t port_dest;
  const struct in_addr ip_src;
  const struct in_addr ip_dest;
};

} // namespace tgrep
