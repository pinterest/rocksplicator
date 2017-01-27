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

#include "tgrep/tcp_flow.h"

#include <arpa/inet.h>
#include <thrift/lib/cpp2/protocol/BinaryProtocol.h>
#include <thrift/lib/cpp2/protocol/CompactProtocol.h>


DEFINE_string(target, "", "The string to grep");
DEFINE_bool(finagle, false, "Grep finagle traffic");

DECLARE_bool(stats);

using namespace apache::thrift::transport;
using namespace apache::thrift;
using namespace apache::thrift::protocol;


namespace tgrep {

const static int kMaxAllowedOutOfOrderTcpSegments = 100;


TcpFlow::TcpFlow(const TcpIdentifier& tcp_identifier) :
  queue_(),
  needed_(0),
  identifier_(),
  next_seq_(),
  seq_to_packet_() {
  identifier_ = folly::stringPrintf("%s:%d => ",
                                    inet_ntoa(tcp_identifier.ip_src),
                                    tcp_identifier.port_src);
  folly::stringAppendf(&identifier_, "%s:%d",
                       inet_ntoa(tcp_identifier.ip_dest),
                       tcp_identifier.port_dest);
}


bool TcpFlow::push_back(std::unique_ptr<Packet> packet,
                        apache::thrift::MessageType* mtype,
                        struct timeval* ts) {
  if (!next_seq_) {
    next_seq_ = folly::make_unique<tcp_seq>(packet->seq);
  }

  if (packet->seq < *next_seq_) {
    return true;
  } else if (packet->seq > *next_seq_) {
    auto seq = packet->seq;
    seq_to_packet_.emplace(seq, std::move(packet));

    if (seq_to_packet_.size() > kMaxAllowedOutOfOrderTcpSegments) {
      clear();
    }

    LOG(INFO) <<
      "libpcap dropped a packet, try to use a larger ring buffer size";
    return false;
  }

  auto len = packet->buf->computeChainDataLength();
  *next_seq_ += len;
  queue_.append(std::move(packet->buf));

  for (auto itor = seq_to_packet_.begin();
       itor != seq_to_packet_.end(); ) {
    if (itor->first < *next_seq_) {
      seq_to_packet_.erase(itor++);
    } else if (itor->first > *next_seq_) {
      break;
    } else {
      len += itor->second->buf->computeChainDataLength();
      *next_seq_ += itor->second->buf->computeChainDataLength();
      queue_.append(std::move(itor->second->buf));
      seq_to_packet_.erase(itor++);
    }
  }

  if (len < needed_) {
    needed_ -= len;
    return true;
  }

  try {
    THeader header;
    auto msg = FLAGS_finagle ?
      extract_msg_finagle(header) : extract_msg_thrift(header);

    if (!msg) {
      return true;
    }
    needed_ = 0;

    if (!FLAGS_target.empty()) {
      auto byteRange = msg->coalesce();
      if (byteRange.find(
        decltype(byteRange)((const unsigned char*)&FLAGS_target[0],
                            FLAGS_target.size())) == std::string::npos) {
        return true;
      }
    }

    if (!FLAGS_stats) {
      std::cout << "**********************************************" << std::endl
                << ctime((const time_t *) &packet->ts.tv_sec)
                << packet->ts.tv_sec << "." << packet->ts.tv_usec << std::endl
                << identifier_ << std::endl;
    } else {
      *ts = packet->ts;
    }

    switch (header.getProtocolId()) {
    case T_BINARY_PROTOCOL:
      {
        apache::thrift::BinaryProtocolReader iprot;

        if (FLAGS_stats) {
          *mtype = get_mtype(std::move(msg), iprot);
        } else {
          print_message(std::cout, std::move(msg), iprot);
        }
        break;
      }
    case T_COMPACT_PROTOCOL:
      {
        apache::thrift::CompactProtocolReader iprot;

        if (FLAGS_stats) {
          *mtype = get_mtype(std::move(msg), iprot);
        } else {
          print_message(std::cout, std::move(msg), iprot);
        }
        break;
      }
    default:
      std::cout << "Unsupported protocol: " <<
        header.getProtocolId() <<
        std::endl;
      return true;
    }
  } catch (const std::exception& e) {
    clear();
    std::cout << "Exception: " << e.what() << std::endl;
  }

  return true;
}

std::unique_ptr<folly::IOBuf> TcpFlow::extract_msg_thrift(THeader& header) {
  std::map<std::string, std::string> read_headers;
  return header.removeHeader(&queue_, needed_, read_headers);
}

std::unique_ptr<folly::IOBuf> TcpFlow::extract_msg_finagle(THeader& header) {
  auto byteRange = const_cast<IOBuf*>(queue_.front())->coalesce();
  const static uint32_t magics[4] = { 0x01000180 /* call */,
                                      0x02000180 /* reply */,
                                      0x03000180 /* exception */,
                                      0x04000180 /* oneway */};
  std::size_t pos;
  for (std::size_t i = 0; i < sizeof(magics) / sizeof(magics[0]); ++i) {
    pos = byteRange.find(decltype(byteRange)((const unsigned char*)&magics[i],
                                             sizeof(magics[i])));
    if (pos != std::string::npos) {
      break;
    }
  }
  if (pos == std::string::npos) {
    // didn't find the magic
    if (byteRange.size() > 16 * 1024 * 1024) {
      // if the msg size is over 16M and we didn't find any finagle magics,
      // it's most likely not finagle traffic
      std::cout << "Did you mean to grep thrift traffic? " << std::endl;
      clear();
      return nullptr;
    }

    needed_ = 1;
    return nullptr;
  }

  queue_.split(pos);
  std::map<std::string, std::string> read_headers;
  return header.removeHeader(&queue_, needed_, read_headers);
}


} // namespace tgrep
