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
#include <memory>
#include <netinet/in.h>
#include <sys/time.h>

#include <folly/io/IOBuf.h>

#include "tgrep/header.h"
#include "tgrep/tcp_identifier.h"

namespace tgrep {

struct Packet {
  Packet(const struct timeval ts_arg,
         std::unique_ptr<folly::IOBuf> buf_arg,
         uint16_t port_src_arg,
         uint16_t port_dest_arg,
         struct in_addr ip_src_arg,
         struct in_addr ip_dest_arg,
         tcp_seq seq_arg) :
    ts(ts_arg),
    buf(std::move(buf_arg)),
    tcp_identifier(port_src_arg,
                   port_dest_arg,
                   ip_src_arg,
                   ip_dest_arg),
    seq(seq_arg) {
  }

  const struct timeval ts;
  std::unique_ptr<folly::IOBuf> buf;
  const TcpIdentifier tcp_identifier;
  const tcp_seq seq;
};


} // namespace tgrep
