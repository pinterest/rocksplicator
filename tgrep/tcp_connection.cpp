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

#include "tgrep/tcp_connection.h"

#include <arpa/inet.h>
#include <iostream>

folly::Histogram<int64_t> g_histogram(1, 0, 1000);
int64_t mismatched_call = 0;
int64_t mismatched_reply = 0;
int64_t total_pairs = 0;

SCOPE_EXIT {
  std::cout << "mismatched call " << mismatched_call << std::endl;
  std::cout << "mismatched reply " << mismatched_reply << std::endl;
  std::cout << "total pairs " << total_pairs << std::endl;
  std::cout << "Global latency stats:" << std::endl;
  std::cout << "P50 " << g_histogram.getPercentileEstimate(0.5) << std::endl;
  std::cout << "P90 " << g_histogram.getPercentileEstimate(0.90) << std::endl;
  std::cout << "P99 " << g_histogram.getPercentileEstimate(0.99) << std::endl;
  std::cout << "P999 " << g_histogram.getPercentileEstimate(0.999) << std::endl;
  std::cout << "P9999 " << g_histogram.getPercentileEstimate(0.9999) << std::endl;
};

namespace tgrep {

TcpConnection::TcpConnection(const TcpIdentifier& id)
    : histogram_(1, 0, 1000) {
  identifier_ = folly::stringPrintf("%s:%d <=> ",
                                    inet_ntoa(id.ip_src), id.port_src);
  folly::stringAppendf(&identifier_, "%s:%d",
                       inet_ntoa(id.ip_dest), id.port_dest);
}

void TcpConnection::push_back(std::unique_ptr<Packet> packet) {
  auto res = flows_.emplace(packet->tcp_identifier, packet->tcp_identifier);
  auto& tcp_flow = res.first->second;


  struct timeval ts;
  apache::thrift::MessageType mtype = (apache::thrift::MessageType)0;

  if (!tcp_flow.push_back(std::move(packet), &mtype, &ts)) {
    // dropped some packets
    flows_.clear();
    last_seen_call_time_.tv_sec = 0;
    return;
  }

  if (mtype != 0) {
    if (mtype == apache::thrift::MessageType::T_CALL) {
      if (last_seen_call_time_.tv_sec != 0) {
        ++ mismatched_call;
      }

      last_seen_call_time_ = ts;
    } else if(mtype == apache::thrift::MessageType::T_REPLY) {
      if (last_seen_call_time_.tv_sec != 0) {
        int64_t start_ms = last_seen_call_time_.tv_sec * 1000 +
          last_seen_call_time_.tv_usec / 1000;
        int64_t end_ms = ts.tv_sec * 1000 + ts.tv_usec / 1000;
        histogram_.addValue(end_ms - start_ms);
        g_histogram.addValue(end_ms - start_ms);
        ++ total_pairs;
        last_seen_call_time_.tv_sec = 0;
      } else {
        ++ mismatched_reply;
      }
    }
  }
}

void TcpConnection::dump_stats() const {
  std::cout << identifier_ <<
    ": P50 " << histogram_.getPercentileEstimate(0.5) <<
    ": P90 " << histogram_.getPercentileEstimate(0.9) <<
    ": P99 " << histogram_.getPercentileEstimate(0.99) <<
    ": P999 " << histogram_.getPercentileEstimate(0.999) <<
    ": P9999 " << histogram_.getPercentileEstimate(0.9999) << std::endl;
}

}
