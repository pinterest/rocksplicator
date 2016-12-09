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
#include <random>
#include <set>
#include <string>
#include <vector>
#include <unordered_map>
#include <utility>

#include "common/file_watcher.h"
#include "common/availability_zone.h"
#include "common/thrift_client_pool.h"
#include "folly/RWSpinLock.h"
#include "folly/SocketAddress.h"
#include "folly/ThreadLocal.h"

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
  std::string az;  // availability zone
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
template <typename ClientType>
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
      const std::string& local_az,
      const std::string& config_path,
      std::function<std::unique_ptr<const ClusterLayout>(std::string)> parser)
      : config_path_(config_path)
      , parser_(std::move(parser))
      , layout_rwlock_()
      , cluster_layout_()
      , local_client_map_(local_az) {
    CHECK(common::FileWatcher::Instance()->AddFile(
      config_path_,
      [this] (std::string content) {
        std::shared_ptr<const ClusterLayout> new_layout(
          parser_(std::move(content)));
        {
          folly::RWSpinLock::WriteHolder write_guard(layout_rwlock_);
          cluster_layout_.swap(new_layout);
        }
      }))
    << "Failed to watch " << config_path_;
    LOG(INFO) << "Local AZ used by ThriftRouter: " << local_az;
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
   *                  if role::ANY is set, we try to return the best matching
   *                  hosts basing on
   *                  (1) Prefer master than slave (2) Prefer local
   *                  than non local.
   * @param quantity  The number of servers requested.
   *                  Return NOT_FOUND if could not find any server matching the
   *                  conditions.
   *                  For ONE and TWO, return BAD_HOST if could not find any GOOD
   *                  server matching the conditions (we may return one server
   *                  for TWO if only one found).
   *                  For ALL, return BAD_HOST if there exists at least one bad
   *                  server matching the conditions.
   *                  The returned host will be put into clients.
   * @param shard     The requested shard
   * @param clients   The out parameter for returned clients, the clients will be
   *                  sorted according the to preference we described above.
   *
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
    std::shared_ptr<const ClusterLayout> layout;
    {
      folly::RWSpinLock::ReadHolder read_guard(layout_rwlock_);
      layout = cluster_layout_;
    }

    if (UNLIKELY(layout == nullptr)) {
      return 0;
    }

    auto itor = layout->segments.find(segment);
    if (itor == layout->segments.end()) {
      return 0;
    }

    return itor->second.shard_to_hosts.size();
  }

  // no copy or move
  ThriftRouter(const ThriftRouter&) = delete;
  ThriftRouter& operator=(const ThriftRouter&) = delete;

 private:
  void updateClusterLayout() {
    std::shared_ptr<const ClusterLayout> layout;
    {
      folly::RWSpinLock::ReadHolder read_guard(layout_rwlock_);
      layout = cluster_layout_;
    }
    local_client_map_.updateClusterLayout(std::move(layout));
  }

  class ThreadLocalClientMap {
   public:
    explicit ThreadLocalClientMap(std::string local_az):
        local_az_(std::move(local_az)) {
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
          shard_to_hosts[shard], role, rotation_counter);
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
     * The function does in the following step to produce an order list of Hosts
     * which is used by getClientsFor to fill the result:
     * (1) Filter by Role.
     * (2) if role::ANY is given, sort Master in front of Slave
     * (3) Within the same type of role, sort Local host in front of foreign hosts.
     * For the rest of hosts, if they have the same role and not in the same az as
     * local host, we randomize them.
     * and multi hosts.
     */
     auto filterByRoleAndSortByPreference(
        const std::vector<std::pair<const Host*, Role>>& host_info,
        Role role, unsigned rotation_counter) {
      std::vector<const Host*> v;
      if (role == Role::ANY) {
        std::vector<const Host*> v_s;
        for (const auto& hi : host_info) {
          if (hi.second == Role::SLAVE) {
            v_s.push_back(hi.first);
          } else if (hi.second == Role::MASTER) {
            v.push_back(hi.first);
          }
        }
        MoveUpLocalAndRotateRemote(&v, rotation_counter);
        MoveUpLocalAndRotateRemote(&v_s, rotation_counter);
        v.insert(v.end(), v_s.begin(), v_s.end());
      } else {
        for (const auto& hi : host_info) {
          if (hi.second == role) {
            v.push_back(hi.first);
          }
        }
        MoveUpLocalAndRotateRemote(&v, rotation_counter);
      }
      return v;
    }

    void MoveUpLocalAndRotateRemote(
            std::vector<const Host*>* v, unsigned rotation_counter) {
      auto find_non_local = [this](const Host* host) {
            return host->az != this->local_az_;
            };
      auto comparator = [this](const Host* h1, const Host* h2) -> bool {
          if (h1->az != h2->az &&
                (h1->az == this->local_az_ || h2->az == this->local_az_)) {
            return h1->az == this->local_az_;
          } else {
            return h1->az < h2->az;
          }
      };
      std::sort(v->begin(), v->end(), comparator);
      auto non_local_it = std::find_if(v->begin(), v->end(), find_non_local);
      auto rotation_vec_size = std::distance(non_local_it, v->end());
      if (rotation_vec_size <= 1) {
        return;
      }
      unsigned rotation_offset = rotation_counter % rotation_vec_size;
      std::rotate(non_local_it, non_local_it + rotation_offset, v->end());
    }

    // TODO(bol) In theory, there is data race in this function. Because the IO
    // thread writes the flag in socket, while we read it without any
    // synchronization. We should remove this function once we are able to get
    // a reliable atomic is_good flag.
    static bool is_client_good(ClientType* client) {
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
                                           &cs.is_good);
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

    const std::string local_az_;
    ThriftClientPool<ClientType> client_pool_;
    folly::ThreadLocal<std::shared_ptr<const ClusterLayout>>
      local_cluster_layout_;
    folly::ThreadLocal<std::unordered_map<folly::SocketAddress,
                                          ClientAndStatus>> clients_;
  };

  const std::string config_path_;
  std::function<std::unique_ptr<const ClusterLayout>(std::string)> parser_;

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
std::unique_ptr<const detail::ClusterLayout> parseConfig(std::string content);

}  // namespace common
