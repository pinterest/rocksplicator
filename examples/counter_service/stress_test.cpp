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

#include <atomic>
#include <thread>
#include <vector>

#include "common/thrift_client_pool.h"
#include "examples/counter_service/thrift/gen-cpp2/Counter.h"
#include "gflags/gflags.h"

DEFINE_string(server_ip, "", "server ip");
DEFINE_int32(server_port, 9090, "server port");
DEFINE_int32(thread_num, 8, "thread number");
DEFINE_bool(verify_results, false, "verify results or not");
DEFINE_int32(key_num_per_thread, 1024, "key num per thread");
DEFINE_int32(allowed_flying_requests, 1024 * 1024, "allowed flying requests");

using ClientType = counter::CounterAsyncClient;

int main(int argc, char** argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  common::ThriftClientPool<ClientType> client_pool;
  folly::MPMCQueue<bool> tokens(FLAGS_allowed_flying_requests);

  std::vector<std::thread> threads;
  threads.reserve(FLAGS_thread_num);
  for (int i = 0; i < FLAGS_thread_num; ++i) {
    threads.emplace_back(std::thread([&client_pool, &tokens, i] () {
          std::string base_key_name = "T" + folly::to<std::string>(i);
          auto client = client_pool.getClient(FLAGS_server_ip, FLAGS_server_port);
          int64_t current_value = 0;

          const apache::thrift::RpcOptions rpc_options;
          counter::BumpRequest bump_request;
          bump_request.counter_name = base_key_name;
          bump_request.counter_delta = 1;
          counter::GetRequest get_request;
          get_request.counter_name = base_key_name;
          counter::GetResponse get_response;
          get_request.counter_name = base_key_name;
          while (true) {
            if (FLAGS_verify_results) {
              ++current_value;
              auto options = rpc_options;
              client->future_bumpCounter(options, bump_request).get();
              options = rpc_options;
              auto get_response = client->future_getCounter(options,
                                                            get_request).get();
              if (get_response.counter_value != current_value) {
                LOG(ERROR) << "Incorrect value " << get_response.counter_value
                           << " expected " << current_value;
              } else if (current_value % 1000) {
                LOG(ERROR) << "Good for the past 1000 requests";
              }
            } else {
              ++current_value;
              bump_request.counter_name = base_key_name +
                folly::to<std::string>(current_value % FLAGS_key_num_per_thread);
              tokens.blockingWrite(true);
              auto options = rpc_options;
              client->future_bumpCounter(options, bump_request).then([&tokens] () {
                  bool b;
                  tokens.read(b);
                });
            }
          }
        }));
  }

  for (auto& thread : threads) {
    thread.join();
  }
}
