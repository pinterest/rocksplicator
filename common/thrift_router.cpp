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

//
// @author bol (bol@pinterest.com)
//

#include "common/thrift_router.h"

#include <boost/algorithm/string.hpp>
#include <string>
#include <utility>
#include <vector>

#include "common/jsoncpp/include/json/json.h"

DEFINE_bool(always_prefer_local_host, false,
            "Always prefer local host when ordering hosts");
DEFINE_int32(min_client_reconnect_interval_seconds, 5,
             "min reconnect interval in seconds");
DEFINE_int64(client_connect_timeout_millis, 100,
             "Timeout for establishing client connection.");


namespace {

bool parseHost(const std::string& str, common::detail::Host* host) {
  std::vector<std::string> tokens;
  folly::split(":", str, tokens);
  if (tokens.size() < 2 || tokens.size() > 3) {
    return false;
  }
  try {
    uint16_t port = atoi(tokens[1].c_str());
    host->addr.setFromIpPort(tokens[0], port);
  } catch (...) {
    return false;
  }

  host->az = (tokens.size() == 3 ? tokens[2] : "unknown");
  return true;
}

bool parseShard(const std::string& str, common::detail::Role* role,
                uint32_t* shard) {
  std::vector<std::string> tokens;
  folly::split(":", str, tokens);
  if (tokens.size() < 1 || tokens.size() > 2) {
    return false;
  }

  try {
    *shard = atoi(tokens[0].c_str());
  } catch (...) {
    return false;
  }
  *role = common::detail::Role::MASTER;
  if (tokens.size() == 2 && tokens[1] == "S") {
    *role = common::detail::Role::SLAVE;
  }

  return true;
}

}  // namespace

namespace common {

std::unique_ptr<const detail::ClusterLayout> parseConfig(std::string content) {
  auto cl = std::make_unique<detail::ClusterLayout>();
  Json::Reader reader;
  Json::Value root;
  if (!reader.parse(content, root) || !root.isObject()) {
    return nullptr;
  }

  static const std::vector<std::string> SHARD_NUM_STRs =
    { "num_leaf_segments", "num_shards" };
  for (const auto& segment : root.getMemberNames()) {
    // for each segment
    const auto& segment_value = root[segment];
    if (!segment_value.isObject()) {
      return nullptr;
    }

    uint32_t shard_number;
    if (segment_value.isMember(SHARD_NUM_STRs[0]) &&
        segment_value[SHARD_NUM_STRs[0]].isInt()) {
      shard_number = segment_value[SHARD_NUM_STRs[0]].asInt();
    } else if (segment_value.isMember(SHARD_NUM_STRs[1]) &&
               segment_value[SHARD_NUM_STRs[1]].isInt()) {
      shard_number = segment_value[SHARD_NUM_STRs[1]].asInt();
    } else {
      LOG(ERROR) << "missing or invalid shard number for " << segment;
      return nullptr;
    }

    cl->segments[segment].shard_to_hosts.resize(shard_number);
    // for each host:port:az
    for (const auto& host_port_az : segment_value.getMemberNames()) {
      if (host_port_az == SHARD_NUM_STRs[0] ||
          host_port_az == SHARD_NUM_STRs[1]) {
        continue;
      }

      detail::Host host;
      if (!parseHost(host_port_az, &host)) {
        LOG(ERROR) << "Invalid host port az " << host_port_az;
        return nullptr;
      }
      const detail::Host* pHost = &*(cl->all_hosts.insert(host).first);
      const auto& shard_list = segment_value[host_port_az];
      // for each shard
      for (Json::ArrayIndex i = 0; i < shard_list.size(); ++i) {
        const auto& shard = shard_list[i];
        if (!shard.isString()) {
          LOG(ERROR) << "Invalid shard list for " << host_port_az;
          return nullptr;
        }

        auto shard_str = shard.asString();
        std::pair<const detail::Host*, detail::Role> p;
        uint32_t shard_id = 0;
        if (!parseShard(shard_str, &p.second, &shard_id) ||
            shard_id >= shard_number) {
          LOG(ERROR) << "Invalid shard " << shard_str;
          return nullptr;
        }
        p.first = pHost;
        cl->segments[segment].shard_to_hosts[shard_id].push_back(p);
      }
    }
  }

  return std::unique_ptr<const detail::ClusterLayout>(std::move(cl));
}

std::unique_ptr<const detail::ClusterLayout>
parseServerset(std::string content) {
  std::vector<std::string> endpoints;
  boost::split(endpoints, content, boost::is_any_of("\n"));
  if (endpoints.empty()) {
    LOG(ERROR) << "Empty serverset";
    return nullptr;
  }
  auto cl = std::make_unique<detail::ClusterLayout>();
  for (const std::string& endpoint: endpoints) {
    detail::Host host;
    if (!parseHost(endpoint, &host)) {
      LOG(ERROR) << "Invalid host port string" << endpoint;
      return nullptr;
    }
    cl->all_hosts.insert(host);
  }
  return std::unique_ptr<const detail::ClusterLayout>(std::move(cl));
}

}  // namespace common
