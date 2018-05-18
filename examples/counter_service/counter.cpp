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

#include <sys/types.h>

#include <string>
#include <memory>
#include <vector>

#include "examples/counter_service/counter_handler.h"
#include "examples/counter_service/counter_router.h"
#include "examples/counter_service/rocksdb_options.h"
#include "examples/counter_service/stats_enum.h"


#include "common/availability_zone.h"
#include "common/stats/stats.h"
#include "common/stats/status_server.h"
#include "gflags/gflags.h"
#include "rocksdb_admin/helix_client.h"
#include "thrift/lib/cpp2/server/ThriftServer.h"


DEFINE_int32(num_worker_threads, 256, "Total number of thrift worker threads");
DEFINE_int32(num_server_io_threads, sysconf(_SC_NPROCESSORS_ONLN),
             "The number of threads for processing server IO");

DEFINE_string(helix_zk_connect_str, "127.0.0.1:2181",
              "ZK cluster connect string");

DEFINE_string(helix_cluster_name, "", "Which helix cluster to join");

DEFINE_bool(read_only_mode, true, "If the cluster does batch uploading only");

DEFINE_string(post_url, "", "The url to post the shard mapping file to");

DECLARE_string(shard_config_path);
DECLARE_int32(port);

using std::chrono::milliseconds;

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  common::Stats::init(counter::getCounterNames(), counter::getMetricNames());

  auto router = std::make_unique<counter::CounterRouter>(
    common::getAvailabilityZone(), FLAGS_shard_config_path);
  auto server = std::make_unique<apache::thrift::ThriftServer>();

  const bool helix_mode = !FLAGS_helix_cluster_name.empty();
  std::unique_ptr<admin::ApplicationDBManager> db_manager;
  if (helix_mode) {
      db_manager = std::make_unique<admin::ApplicationDBManager>();
  }

  auto handler = std::make_unique<counter::CounterHandler>(
    std::move(db_manager), counter::GetRocksdbOptions,
    std::move(router), rocksdb::WriteOptions(), rocksdb::ReadOptions());

  server->setInterface(std::move(handler));

  server->setPort(FLAGS_port);
  server->setIdleTimeout(std::chrono::milliseconds(0));
  server->setTaskExpireTime(std::chrono::milliseconds(0));
  server->setNPoolThreads(FLAGS_num_worker_threads);
  server->setNWorkerThreads(FLAGS_num_server_io_threads);

  common::StatusServer::StartStatusServer();

  if (helix_mode) {
      auto az = common::getAvailabilityZone();
      auto pg = az; // put your own placement query function here if you want
      CHECK(!az.empty()) << "Failed to get az";
      // e.g., "az=us-east-1a,pg=placement_group1"
      auto domain = "az=" + az + ",pg=" + (pg.empty() ? az : pg);
      LOG(INFO) << "Joining Helix cluster with domain " << domain;
      admin::JoinCluster(FLAGS_helix_zk_connect_str,
                         FLAGS_helix_cluster_name,
                         FLAGS_read_only_mode ? "OnlineOffline" : "MasterSlave",
                         domain,
                         "/mnt/counter/bin/",
                         FLAGS_post_url);
  }

  LOG(INFO) << "Starting server at port " << FLAGS_port;
  server->serve();
  
}
