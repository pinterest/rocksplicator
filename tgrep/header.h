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

/* ethernet headers are always exactly 14 bytes [1] */
const uint32_t SIZE_ETHERNET = 14;

/* IP header */
struct sniff_ip {
  uint8_t  ip_vhl;                 /* version << 4 | header length >> 2 */
  uint8_t  ip_tos;                 /* type of service */
  uint16_t ip_len;                 /* total length */
  uint16_t ip_id;                  /* identification */
  uint16_t ip_off;                 /* fragment offset field */
#define IP_RF 0x8000            /* reserved fragment flag */
#define IP_DF 0x4000            /* dont fragment flag */
#define IP_MF 0x2000            /* more fragments flag */
#define IP_OFFMASK 0x1fff       /* mask for fragmenting bits */
  uint8_t  ip_ttl;                 /* time to live */
  uint8_t  ip_p;                   /* protocol */
  uint16_t ip_sum;                 /* checksum */
  struct in_addr ip_src;          /* source and dest address */
  struct in_addr ip_dst;  
};
#define IP_HL(ip)               (((ip)->ip_vhl) & 0x0f)
#define IP_V(ip)                (((ip)->ip_vhl) >> 4)

/* TCP header */
using tcp_seq = u_int;

struct sniff_tcp {
  uint16_t th_sport;               /* source port */
  uint16_t th_dport;               /* destination port */
  tcp_seq th_seq;                 /* sequence number */
  tcp_seq th_ack;                 /* acknowledgement number */
  uint8_t  th_offx2;               /* data offset, rsvd */
#define TH_OFF(th)      (((th)->th_offx2 & 0xf0) >> 4)
  uint8_t  th_flags;
        #define TH_FIN  0x01
        #define TH_SYN  0x02
        #define TH_RST  0x04
        #define TH_PUSH 0x08
        #define TH_ACK  0x10
        #define TH_URG  0x20
        #define TH_ECE  0x40
        #define TH_CWR  0x80
#define TH_FLAGS        (TH_FIN|TH_SYN|TH_RST|TH_ACK|TH_URG|TH_ECE|TH_CWR)
  uint16_t th_win;                 /* window */
  uint16_t th_sum;                 /* checksum */
  uint16_t th_urp;                 /* urgent pointer */
};

} // namespace tgrep
