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

#include "tgrep/tcp_identifier.h"

namespace tgrep {

TcpIdentifier TcpIdentifier::getConnectionIdentifier() const {
  TcpIdentifier opposite(port_dest, port_src, ip_dest, ip_src);
  return opposite < *this ? opposite : *this;
}

bool TcpIdentifier::operator < (const TcpIdentifier& tcp) const {
  if (port_src < tcp.port_src) {
    return true;
  } else if (port_src > tcp.port_src) {
    return false;
  }

  if (port_dest < tcp.port_dest) {
    return true;
  } else if (port_dest > tcp.port_dest) {
    return false;
  }

  if (ip_src.s_addr < tcp.ip_src.s_addr) {
    return true;
  } else if (ip_src.s_addr > tcp.ip_src.s_addr) {
    return false;
  }

  if (ip_dest.s_addr < tcp.ip_dest.s_addr) {
    return true;
  } else if (ip_dest.s_addr > tcp.ip_dest.s_addr) {
    return false;
  }

  return false;
}

} // namespace tgrep
