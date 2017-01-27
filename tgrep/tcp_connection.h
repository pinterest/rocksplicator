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

#include <map>

#include "folly/stats/Histogram.h"
#include "tgrep/packet.h"
#include "tgrep/tcp_flow.h"
#include "tgrep/tcp_identifier.h"

namespace tgrep {

class TcpConnection {
public:
  TcpConnection(const TcpIdentifier& id);

  void push_back(std::unique_ptr<Packet> packet);

  void dump_stats() const;

private:
  std::string identifier_;
  std::map<TcpIdentifier, TcpFlow> flows_;
  struct timeval last_seen_call_time_;
  folly::Histogram<int64_t> histogram_;
};

}
