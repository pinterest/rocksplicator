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
#include "thrift/lib/cpp2/server/ThriftServer.h"


DEFINE_int32(num_worker_threads, 256, "Total number of thrift worker threads");
DEFINE_int32(num_server_io_threads, sysconf(_SC_NPROCESSORS_ONLN),
             "The number of threads for processing server IO");

DECLARE_string(shard_config_path);
DECLARE_int32(port);

using std::chrono::milliseconds;

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  common::Stats::init(counter::getCounterNames(), counter::getMetricNames());

  auto router = std::make_unique<counter::CounterRouter>(
    common::getAvailabilityZone(), FLAGS_shard_config_path);
  auto server = std::make_unique<apache::thrift::ThriftServer>();

  auto handler = std::make_unique<counter::CounterHandler>(
    nullptr, counter::GetRocksdbOptions,
    std::move(router), rocksdb::WriteOptions(), rocksdb::ReadOptions());

  server->setInterface(std::move(handler));

  server->setPort(FLAGS_port);
  server->setIdleTimeout(std::chrono::milliseconds(0));
  server->setTaskExpireTime(std::chrono::milliseconds(0));
  server->setNPoolThreads(FLAGS_num_worker_threads);
  server->setNWorkerThreads(FLAGS_num_server_io_threads);

  common::StatusServer::StartStatusServer();
  LOG(INFO) << "Starting server at port " << FLAGS_port;
  server->serve();
  
}
