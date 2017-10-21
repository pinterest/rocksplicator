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

#pragma once

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <unordered_map>
#include <utility>

#include "common/file_watcher.h"
#include "common/network_util.h"
#include "common/thrift_client_pool.h"
#include "folly/RWSpinLock.h"
#include "folly/SocketAddress.h"
#include "folly/ThreadLocal.h"

DECLARE_bool(always_prefer_local_host);
DECLARE_int32(min_client_reconnect_interval_seconds);
DECLARE_int64(client_connect_timeout_millis);

namespace common {

namespace detail {

enum class Role { MASTER, SLAVE, ANY };
enum class Quantity { ONE, TWO, ALL };
enum ReturnCode { OK, NOT_FOUND, BAD_HOST, UNKNOWN_SEGMENT, UNKNOWN_SHARD };
using ShardID = uint32_t;
using SegmentName = std::string;

struct Host {
  bool operator<(const Host& other) const {
    return addr < other.addr;
  }

  folly::SocketAddress addr;
  // The hosts' longest common prefix length with local group
  std::unordered_map<std::string, uint16_t> groups_prefix_lengths;
};

struct SegmentInfo {
  // there are totally shard_to_hosts.size() shards in this segment.
  // shard_to_hosts[i] contains all host info for shard i.
  // Host* refers to a host in ClusterLayout.all_hosts
  std::vector<std::vector<std::pair<const Host*, Role>>> shard_to_hosts;
};

struct ClusterLayout {
  std::map<SegmentName, SegmentInfo> segments;
  std::set<Host> all_hosts;
};

}  // namespace detail

/*
 * A router for sharded thrift services.
 * It returns thrift client objects based on the requested conditions.
 * All interfaces of ThriftRouter are thread safe.
 */
template <typename ClientType, bool USE_BINARY_PROTOCOL = false>
class ThriftRouter {
 public:
  using Role = detail::Role;
  using Quantity = detail::Quantity;
  using ReturnCode = detail::ReturnCode;
  using ShardID = detail::ShardID;
  using SegmentName = detail::SegmentName;
  using Host = detail::Host;
  using SegmentInfo = detail::SegmentInfo;
  using ClusterLayout = detail::ClusterLayout;

  /*
   * @param config_path  The config file path
   * @param parser       User provided parser to parse config file content into
   *                     an in memory READONLY ClusterLayout structure.
   */
  ThriftRouter(
      const std::string& local_group,
      const std::string& config_path,
      std::function<std::unique_ptr<const ClusterLayout>(
        std::string, const std::string&)> parser)
      : config_path_(config_path)
      , parser_(std::move(parser))
      , layout_rwlock_()
      , cluster_layout_()
      , local_client_map_() {
    CHECK(common::FileWatcher::Instance()->AddFile(
      config_path_,
      [this, local_group] (std::string content) {
        std::shared_ptr<const ClusterLayout> new_layout(
          parser_(std::move(content), local_group));
        {
          folly::RWSpinLock::WriteHolder write_guard(layout_rwlock_);
          cluster_layout_.swap(new_layout);
        }
      }))
    << "Failed to watch " << config_path_;
    LOG(INFO) << "Local Group used by ThriftRouter: " << local_group;
  }

  ~ThriftRouter() {
    if (!common::FileWatcher::Instance()->RemoveFile(config_path_)) {
      LOG(ERROR) << "Failed to stop watching " << config_path_;
    }
  }


  /*
   * Get clients pointing to the server(s) matching the request conditions.
   * @note It is encouraged and performance-wise OK to call getClientsFor() for
   *       every RPC.
   * @param segment   The requested segment
   * @param role      The requested server role.
   * @param quantity  The number of servers requested.
   *                  Return NOT_FOUND if could not find any server matching the
   *                  conditions.
   *                  For ONE and TWO, return BAD_HOST if could not find ANY
   *                  good host matching the conditions (we may return one
   *                  server for TWO if only one found).
   *                  For ALL, return BAD_HOST if there exists any bad server
   *                  matching the conditions.
   * @param shard     The requested shard
   * @param clients   The out parameter for returned clients, the clients will be
   *                  sorted according to the criteria below.
   *
   *                  If role == ANY && !FLAGS_always_prefer_local_host, we sort
   *                  the returned hosts first by
   *                  (1) Prefer master to slave, then (2) Prefer local to
   *                  non-local.
   *
   *                  Otherwise, we sort them by (2) only
   *
   *                  If two hosts equal according to the sorting criteria, we
   *                  randomly order them
   */
  ReturnCode getClientsFor(const std::string& segment,
                           const Role role,
                           const Quantity quantity,
                           const ShardID shard,
                           std::vector<std::shared_ptr<ClientType>>* clients) {
    std::map<ShardID, std::vector<std::shared_ptr<ClientType>>>
      shard_to_clients;
    shard_to_clients[shard];
    auto ret = getClientsFor(segment, role, quantity, &shard_to_clients);
    *clients = std::move(shard_to_clients[shard]);
    return ret;
  }

  /*
   * Similar to getClientsFor() above. The difference is it gets clients for
   * multiple shards. This allows us to get clients for multiple shards based on
   * a single config file snapshot (There might be config file changes between
   * two calls of getClientsFor()).
   * @note shard_to_clients is an in-out parameter
   *
   * Trying to fill clients for as many shards as possible.
   * If all shards are successfully fulfilled with the required clients,
   * OK is returned.
   * Otherwise, the error code for the last failure shard is returned.
   */
  ReturnCode getClientsFor(
      const std::string& segment,
      const Role role,
      const Quantity quantity,
      std::map<ShardID, std::vector<std::shared_ptr<ClientType>>>*
        shard_to_clients) {
    updateClusterLayout();
    return local_client_map_.getClientsFor(segment, role, quantity,
                                           shard_to_clients);
  }

  uint32_t getShardNumberFor(const std::string& segment) {
    const auto layout = getClusterLayout();

    if (UNLIKELY(layout == nullptr)) {
      return 0;
    }

    auto itor = layout->segments.find(segment);
    if (itor == layout->segments.end()) {
      return 0;
    }

    return itor->second.shard_to_hosts.size();
  }

  /*
   * This function is AZ unaware. It returns the total number of hosts
   * within the segment and shard.
   */
  uint32_t getHostNumberFor(const std::string& segment, const ShardID shard) {
    const auto layout = getClusterLayout();

    if (UNLIKELY(layout == nullptr)) {
      return 0;
    }

    auto itor = layout->segments.find(segment);
    if (itor == layout->segments.end()) {
      return 0;
    }

    if (UNLIKELY(shard >= itor->second.shard_to_hosts.size())) {
      return 0;
    }

    return itor->second.shard_to_hosts[shard].size();
  }

  // no copy or move
  ThriftRouter(const ThriftRouter&) = delete;
  ThriftRouter& operator=(const ThriftRouter&) = delete;

 private:
  std::shared_ptr<const ClusterLayout> getClusterLayout() {
    folly::RWSpinLock::ReadHolder read_guard(layout_rwlock_);
    return cluster_layout_;
  }

  void updateClusterLayout() {
    local_client_map_.updateClusterLayout(getClusterLayout());
  }

  class ThreadLocalClientMap {
   public:
    explicit ThreadLocalClientMap() {
      *local_cluster_layout_ = std::make_shared<const ClusterLayout>();
    }

    ReturnCode getClientsFor(
        const std::string& segment,
        const Role role,
        const Quantity quantity,
        std::map<ShardID, std::vector<std::shared_ptr<ClientType>>>*
          shard_to_clients) {
      auto& segments = (*local_cluster_layout_)->segments;
      auto itor = segments.find(segment);
      if (itor == segments.end()) {
        LOG(ERROR) << "Unknown segment: " << segment;
        return ReturnCode::UNKNOWN_SEGMENT;
      }
      // get all hosts involved, and create or fix clients for them.
      std::set<const Host*> hosts;
      thread_local unsigned rotation_counter = 0;
      rotation_counter++;
      auto& shard_to_hosts = itor->second.shard_to_hosts;
      for (const auto& s_c : *shard_to_clients) {
        if (s_c.first >= shard_to_hosts.size()) {
          LOG(ERROR) << "Unknown shard: " << s_c.first;
          return ReturnCode::UNKNOWN_SHARD;
        }

        for (const auto& p : shard_to_hosts[s_c.first]) {
          hosts.insert(p.first);
        }
      }
      createOrFixClientsFor(hosts);
      // Trying to fill clients for as many shards as possible.
      // If all shards are successfully fulfilled with the required clients,
      // OK is returned.
      // Otherwise, the error code for the last failure shard is returned.
      auto ret = ReturnCode::OK;
      for (auto& s_c : *shard_to_clients) {
        // find clients for shard
        auto shard = s_c.first;
        auto& clients = s_c.second;
        clients.clear();
        auto hosts_for_shard = filterByRoleAndSortByPreference(
          shard_to_hosts[shard], role, rotation_counter, segment);
        if (hosts_for_shard.empty()) {
          LOG(ERROR) << "Could not find hosts for shard " << shard;
          ret = ReturnCode::NOT_FOUND;
          continue;
        }
        auto sz = hosts_for_shard.size();
        filterBadHosts(&hosts_for_shard);
        if (sz != hosts_for_shard.size() && quantity == Quantity::ALL) {
          // exist some bad hosts, and we want all of them
          LOG(ERROR) << "There is at least one bad host for shard " << shard;
          ret = ReturnCode::BAD_HOST;
          continue;
        }

        if (hosts_for_shard.empty()) {
          LOG(ERROR) << "We could not find any good host for shard " << shard;
          ret = ReturnCode::BAD_HOST;
          continue;
        }

        for (const auto host : hosts_for_shard) {
          clients.push_back((*clients_)[host->addr].client);
          if (quantity == Quantity::ONE) {
            break;
          }

          if (quantity == Quantity::TWO && clients.size() == 2) {
            break;
          }
        }
      }

      return ret;
    }

    void updateClusterLayout(std::shared_ptr<const ClusterLayout> layout) {
      if (layout == *local_cluster_layout_ || layout == nullptr) {
        return;
      }

      // store the new cluster layout
      *local_cluster_layout_ = std::move(layout);

      // remove clients for non-existing servers
      const auto& hosts = (*local_cluster_layout_)->all_hosts;
      Host host;
      for (auto itor = clients_->begin(); itor != clients_->end();) {
        host.addr = itor->first;
        if (hosts.find(host) != hosts.end()) {
          ++itor;
        } else {
          itor = clients_->erase(itor);
        }
      }
    }

   private:
    /*
     * Filter hosts by role, and then sort them according to the following rules
     *
     * If role == ANY && !FLAGS_always_prefer_local_host, we sort the returned
     * hosts first by
     * (1) Prefer master to slave, then (2) Prefer local to non-local.
     *
     * Otherwise, we sort them by (2) only
     *
     * If two hosts equal according to the sorting criteria, we randomly order
     * them
     */
    auto filterByRoleAndSortByPreference(
        const std::vector<std::pair<const Host*, Role>>& host_info,
        const Role role,
        const unsigned rotation_counter,
        const std::string& segment) {
      std::vector<const Host*> v;
      if (role == Role::ANY && !FLAGS_always_prefer_local_host) {
        // prefer MASTER, then prefer groups with longer common prefix length
        std::vector<const Host*> v_s;
        for (const auto& hi : host_info) {
          if (hi.second == Role::SLAVE) {
            v_s.push_back(hi.first);
          } else if (hi.second == Role::MASTER) {
            v.push_back(hi.first);
          }
        }
        RankHostsByGroupPrefixLength(&v, rotation_counter, segment);
        RankHostsByGroupPrefixLength(&v_s, rotation_counter, segment);
        v.insert(v.end(), v_s.begin(), v_s.end());
      } else {
        // prefer local
        for (const auto& hi : host_info) {
          if (role == Role::ANY || hi.second == role) {
            v.push_back(hi.first);
          }
        }
        RankHostsByGroupPrefixLength(&v, rotation_counter, segment);
      }
      return v;
    }

    void RankHostsByGroupPrefixLength(
        std::vector<const Host*>* v,
        const unsigned rotation_counter,
        const std::string& segment) {
      auto comparator = [this, &segment, &rotation_counter]
        (const Host* h1, const Host* h2) -> bool {
        // Descending order
        auto s1 = h1->groups_prefix_lengths.at(segment);
        auto s2 = h2->groups_prefix_lengths.at(segment);
        if (s1 == s2) {
          auto hash1 = std::hash<std::string>{}(
            std::to_string(reinterpret_cast<uint64_t>(h1) ^ rotation_counter));
          auto hash2 = std::hash<std::string>{}(
            std::to_string(reinterpret_cast<uint64_t>(h2) ^ rotation_counter));
          return hash1 < hash2;
        } else {
          return s1 > s2;
        }
      };
      std::sort(v->begin(), v->end(), comparator);
    }

    // TODO(bol) In theory, there is data race in this function. Because the IO
    // thread writes the flag in socket, while we read it without any
    // synchronization. We should remove this function once we are able to get
    // a reliable atomic is_good flag.
    static bool is_client_good(ClientType* client) {
      if (client == nullptr) {
        return false;
      }

      auto transport = dynamic_cast<apache::thrift::HeaderClientChannel*>
        (client->getChannel())->getTransport();

      auto socket = dynamic_cast<apache::thrift::async::TAsyncSocket*>
        (transport);

      return socket->good();
    }

    void createOrFixClientsFor(const std::set<const Host*>& hosts) {
      for (auto host : hosts) {
        auto itor = clients_->find(host->addr);
        if (itor != clients_->end() &&
            is_client_good(itor->second.client.get())) {
            // has the client and it is good
          continue;
        }

        if (itor != clients_->end() &&
            itor->second.create_time +
            FLAGS_min_client_reconnect_interval_seconds > now()) {
          // has a bad client and it is too soon to reconnect
          continue;
        }

        // either don't have the client or we need to fix the bad client
        auto& cs = (*clients_)[host->addr];
        cs.client = client_pool_.getClient(host->addr,
                                           FLAGS_client_connect_timeout_millis,
                                           &cs.is_good,
                                           false /* aggressively */);
        cs.create_time = now();
      }
    }

    static uint64_t now() {
      return std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    }

    void filterBadHosts(std::vector<const Host*>* hosts) {
      auto itor = std::remove_if(
        hosts->begin(), hosts->end(),
        [this] (const Host* host) {
          return !is_client_good((*clients_)[host->addr].client.get());
        });
      hosts->erase(itor, hosts->end());
    }

    struct ClientAndStatus {
      std::shared_ptr<ClientType> client;
      const std::atomic<bool>* is_good;
      uint64_t create_time;
    };

    ThriftClientPool<ClientType, USE_BINARY_PROTOCOL> client_pool_;
    folly::ThreadLocal<std::shared_ptr<const ClusterLayout>>
      local_cluster_layout_;
    folly::ThreadLocal<std::unordered_map<folly::SocketAddress,
                                          ClientAndStatus>> clients_;
  };

  const std::string config_path_;
  std::function<std::unique_ptr<const ClusterLayout>(
    std::string, const std::string&)> parser_;

  // TODO(bol) use atomic<shared_ptr<>> once move to gcc 5.1
  folly::RWSpinLock layout_rwlock_;
  std::shared_ptr<const ClusterLayout> cluster_layout_;
  ThreadLocalClientMap local_client_map_;
};

/*
 * Parse a stateful sharded service's config.
 *
  "{"
  "  \"user_pins\": {"
  "  \"num_leaf_segments\": 3,"
  "  \"127.0.0.1:8090\": [\"00000\", \"00001\", \"00002\"],"
  "  \"127.0.0.1:8091\": [\"00002\"],"
  "  \"127.0.0.1:8092\": [\"00002\"]"
  "   },"
  "  \"interest_pins\": {"
  "  \"num_leaf_segments\": 2,"
  "  \"127.0.0.1:8090\": [\"00000\"],"
  "  \"127.0.0.1:8091\": [\"00001\"]"
  "   }"
  "}";

  "{"
  "  \"user_pins\": {"
  "  \"num_leaf_segments\": 3,"
  "  \"127.0.0.1:8090:zone_a\": [\"00002:M\", \"00000:M\"],"
  "  \"127.0.0.1:8091:\": [\"00000:S\", \"00001:M\", \"00002:M\"],"
  "  \"127.0.0.1:8092:zone_c\": [\"00001:S\", \"00002:M\"]"
  "   },"
  "  \"interest_pins\": {"
  "  \"num_leaf_segments\": 2,"
  "  \"127.0.0.1:8090:zone_a\": [\"00000:M\"],"
  "  \"127.0.0.1:8091:\": [\"00001:S\"]"
  "   }"
  "}";
 */
std::unique_ptr<const detail::ClusterLayout> parseConfig(
  std::string content, const std::string& local_group);

}  // namespace common
