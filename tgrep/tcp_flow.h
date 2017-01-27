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

#include <iostream>
#include <ostream>
#include <map>
#include <memory>
#include <string>

#include <folly/io/IOBufQueue.h>
#include <folly/String.h>
#include <thrift/lib/cpp/transport/THeader.h>

#include "tgrep/packet.h"
#include "tgrep/tcp_identifier.h"
#include "tgrep/thrift_utils.h"


namespace tgrep {


class TcpFlow {
public:
  // implicit
  TcpFlow(const TcpIdentifier& tcp_identifier);

  bool push_back(std::unique_ptr<Packet> packet,
                 apache::thrift::MessageType* mtype = nullptr,
                 struct timeval* ts = nullptr);

  bool empty() {
    return queue_.empty() && seq_to_packet_.empty();
  }

private:
  void clear(){
    needed_ = 0;
    auto tmp = queue_.move();
    seq_to_packet_.clear();
    next_seq_.reset(nullptr);
  }

  std::unique_ptr<folly::IOBuf> extract_msg_thrift(apache::thrift::transport::THeader& header);

  std::unique_ptr<folly::IOBuf> extract_msg_finagle(apache::thrift::transport::THeader& header);

  template <typename Protocol_>
  static apache::thrift::MessageType get_mtype(std::unique_ptr<folly::IOBuf> msg, Protocol_& iprot) {
    std::string fname;
    apache::thrift::MessageType mtype;
    int32_t protoSeqId = 0;

    iprot.setInput(msg.get());
    iprot.readMessageBegin(fname, mtype, protoSeqId);
    return mtype;
  }

  template <typename Protocol_>
  static void print_message(std::ostream& os, std::unique_ptr<folly::IOBuf> msg, Protocol_& iprot) {
    std::string fname;
    apache::thrift::MessageType mtype;
    apache::thrift::protocol::TType ftype;
    int16_t fid;
    int32_t protoSeqId = 0;

    iprot.setInput(msg.get());
    iprot.readMessageBegin(fname, mtype, protoSeqId);

    os << msgTypeName(mtype) << " " << fname << " seq#: " << protoSeqId << std::endl;

    while (true) {
      iprot.readFieldBegin(fname, ftype, fid);
      if (ftype  == TType::T_STOP) {
        break;
      }
      os << fid << ": " << dataTypeName(ftype) << std::endl;
      print(std::cout, iprot, ftype);
    }

    iprot.readMessageEnd();
  }

  folly::IOBufQueue queue_;
  size_t needed_;
  std::string identifier_;
  std::unique_ptr<tcp_seq> next_seq_;
  std::map<tcp_seq, std::unique_ptr<Packet>> seq_to_packet_;
};


} // namespace tgrep

