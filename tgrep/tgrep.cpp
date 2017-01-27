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

#include <iostream>
#include <thread>

#include <folly/io/IOBuf.h>
#include <folly/Memory.h>
#include <folly/MPMCQueue.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <pcap.h>
#include <signal.h>


#include "tgrep/header.h"
#include "tgrep/tcp_connection.h"
#include "tgrep/tcp_flow.h"
#include "tgrep/thrift_utils.h"

DEFINE_string(dev, "eth0", "The device to sniff");
DEFINE_string(file, "", "The file to load data");
DEFINE_int32(snaplen, 512 * 1024, "snaplen for pcap");
DEFINE_int32(pcap_buffer_size, 128 * 1024 * 1024, "Ring buffer size in bytes");
DEFINE_bool(stats, false, "Show stats only");

using namespace tgrep;

folly::MPMCQueue<std::unique_ptr<Packet>> packet_queue(5 * 1024);

auto deleter = [] (pcap_t * handle) {
  if (handle) {
    pcap_close(handle);
  }
};

std::unique_ptr<pcap_t, decltype(deleter)> handle(nullptr, std::move(deleter));

void sig_handler(int) {
  std::cout << "will exit..." << std::endl;
  if (handle.get()) {
    pcap_breakloop(handle.get());
  }
}

void process_packets() {
  std::unique_ptr<Packet> packet;
  std::map<TcpIdentifier, TcpFlow> flows;
  std::map<TcpIdentifier, TcpConnection> connections;

  SCOPE_EXIT {
    for (const auto& it : connections) {
      it.second.dump_stats();
    }
  };

  while (true) {
    packet_queue.blockingRead(packet);
    if (!packet) {
      std::cout << "Done, bye!" << std::endl;
      break;
    }

    if (FLAGS_stats) {
      const auto& id = packet->tcp_identifier.getConnectionIdentifier();
      auto res = connections.emplace(id, id);
      auto& tcp_connection = res.first->second;

      tcp_connection.push_back(std::move(packet));
    } else {
      auto res = flows.emplace(packet->tcp_identifier, packet->tcp_identifier);
      auto& tcp_flow = res.first->second;
      if (!tcp_flow.push_back(std::move(packet)) || tcp_flow.empty()) {
        flows.erase(res.first);
      }
    }
  }
}

void packet_callback(u_char*, const struct pcap_pkthdr* header, const u_char* packet) {
  // TODO(bol): verify IP and TCP checksum for incoming packets
  int size_ip;
  int size_tcp;
  const struct sniff_ip* ip;
  const struct sniff_tcp* tcp;
  const void* payload;

  if (header->len < SIZE_ETHERNET + 20) {
    LOG(ERROR) << "less than ip header length";
    return;
  }

  ip = (const struct sniff_ip*)(packet + SIZE_ETHERNET);
  size_ip = IP_HL(ip)*4;
  if (size_ip < 20) {
    LOG(ERROR) << "Invalid IP header length: " << size_ip;
    return;
  }

  if (header->len < SIZE_ETHERNET + size_ip + 20) {
    LOG(ERROR) << "less than tcp header length";
    return;
  }

  tcp = (const struct sniff_tcp*)(packet + SIZE_ETHERNET + size_ip);
  size_tcp = TH_OFF(tcp) * 4;
  if (size_tcp < 20) {
    LOG(ERROR) << "Invalid TCP header length: " << size_tcp;
    return;
  }
  uint32_t total_header_size = SIZE_ETHERNET + size_ip + size_tcp;
  if (header->len < total_header_size) {
    LOG(ERROR) << "Invalid packet lenth: " << header->len <<
      " while total header length: " << total_header_size;
    return;
  } else if (header->len == total_header_size) {
    return;
  }

  payload = static_cast<const void*>(packet + total_header_size);
  auto sz = ntohs(ip->ip_len) - (size_ip + size_tcp);

  packet_queue.blockingWrite(folly::make_unique<Packet>(header->ts,
                                                        folly::IOBuf::copyBuffer(payload, sz),
                                                        ntohs(tcp->th_sport),
                                                        ntohs(tcp->th_dport),
                                                        ip->ip_src,
                                                        ip->ip_dst,
                                                        ntohl(tcp->th_seq)));
}

int main(int argc, char* argv[]) {
  ::google::ParseCommandLineFlags(&argc, &argv, true);
  ::google::InitGoogleLogging(argv[0]);

  char errbuf[PCAP_ERRBUF_SIZE];

  if (FLAGS_stats) {
    signal(SIGINT, &sig_handler);
  }

  if (FLAGS_file.empty()) {
    handle.reset(::pcap_create(FLAGS_dev.c_str(), errbuf));
    //    handle.reset(::pcap_open_live(FLAGS_dev.c_str(), FLAGS_snaplen, 0, -1, errbuf));
  } else {
    handle.reset(::pcap_open_offline(FLAGS_file.c_str(), errbuf));
  }


  if (!handle) {
    LOG(ERROR) << "Could not open device/file " << FLAGS_dev << "/" << FLAGS_file << " " << errbuf;
    return -1;
  }

  if (FLAGS_file.empty()) {
    if (::pcap_set_buffer_size(handle.get(), FLAGS_pcap_buffer_size) ||
        ::pcap_set_snaplen(handle.get(), FLAGS_snaplen) ||
        ::pcap_set_promisc(handle.get(), 0) ||
        ::pcap_set_timeout(handle.get(), -1) ||
        ::pcap_activate(handle.get())) {
      LOG(ERROR) << "Failed to prepare pcap handler";
      return -1;
    }
  }

  std::string filter_str = "tcp ";

  for (int i = 1; i < argc; ++i) {
    filter_str += argv[i];
    filter_str.push_back(' ');
  }

  struct bpf_program fp;
  if (pcap_compile(handle.get(), &fp, filter_str.c_str(), 1, PCAP_NETMASK_UNKNOWN)) {
    LOG(ERROR) << "Could not compile filter string: " << filter_str;
    return -1;
  }

  if (pcap_setfilter(handle.get(), &fp)) {
    LOG(ERROR) << "Could not set filter: " << pcap_geterr(handle.get());
    return -1;
  }

  std::thread t(process_packets);

  if (pcap_loop(handle.get(), 0, packet_callback, nullptr) == -1) {
    LOG(ERROR) << "pcap_loop failed " << errno;
    return -1;
  }

  packet_queue.blockingWrite(nullptr);
  t.join();
}
