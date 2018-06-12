/// Copyright 2018 Pinterest Inc.
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
// @author angxu (angxu@pinterest.com)
//

#pragma once

#include <cstdint>
#include <vector>

#include "folly/io/async/AsyncSignalHandler.h"
#include "thrift/lib/cpp2/server/ThriftServer.h"

namespace common {

/*
 * A handler to perform graceful shutdown on a ThriftServer when a user-provided
 * signal number is caught.
 *
 * This handler should be registered in the same thread as what will call
 * server->serve() later.
 *
 * NB: the program who uses GracefulShutdownHandler should NOT register other
 * AsyncSignalHandler anywhere else through out the program!
 */
class GracefulShutdownHandler : public folly::AsyncSignalHandler {
 public:
  explicit GracefulShutdownHandler(
    std::shared_ptr<apache::thrift::ThriftServer> server);

  void RegisterPreShutdownHandler(std::function<void()> handler);
  void RegisterPostShutdownHandler(std::function<void()> handler);
  void RegisterShutdownSignal(int signum);

  void signalReceived(int signum) noexcept override;

 private:
  void InitiateGracefulShutdown();
  // the thrift server to perfrom graceful shutdown
  std::shared_ptr<apache::thrift::ThriftServer> server_;
  // a list of pre-shutdown handlers
  std::vector<std::function<void()>> pre_shutdown_handlers_;
  // a list of post-shutdown handlers
  std::vector<std::function<void()>> post_shutdown_handlers_;
};

}  // namespace common
