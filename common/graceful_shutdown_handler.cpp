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

#include "common/graceful_shutdown_handler.h"

#include <csignal>
#include "glog/logging.h"


namespace common {

GracefulShutdownHandler::GracefulShutdownHandler(
    std::shared_ptr<apache::thrift::ThriftServer> server)
  : AsyncSignalHandler(server->getEventBaseManager()->getEventBase())
  , server_(std::move(server))
  , pre_shutdown_handlers_()
  , post_shutdown_handlers_() {
}

void GracefulShutdownHandler::signalReceived(int signum) noexcept {
  LOG(INFO) << "Received signal " << signum;
  LOG(INFO) << "Initiating graceful shutdown...";
  InitiateGracefulShutdown();
}

void GracefulShutdownHandler::RegisterPreShutdownHandler(
    std::function<void()> handler) {
  pre_shutdown_handlers_.push_back(std::move(handler));
}

void GracefulShutdownHandler::RegisterPostShutdownHandler(
    std::function<void()> handler) {
  post_shutdown_handlers_.push_back(std::move(handler));
}

void GracefulShutdownHandler::RegisterShutdownSignal(int signum) {
  registerSignalHandler(signum);
}

void GracefulShutdownHandler::InitiateGracefulShutdown() {
  // first, invoke pre-shutdown handlers if there is any
  auto ritr = pre_shutdown_handlers_.rbegin();
  for ( ; ritr != pre_shutdown_handlers_.rend(); ++ritr) {
    (*ritr)();
  }
  // stop listening
  server_->stopListening();
  LOG(INFO) << "Stopped listening.";
  // let existing work to finish, then shutdown workers.
  // it's okay to call join() on ThreadManager even when the
  // stopWorkersOnStopListening_ flag is set.
  server_->getThreadManager()->join();
  LOG(INFO) << "Stopped worker thread pool.";
  // no new requests will be accepted from now on, so it should be safe for us
  // to perform ordered shutdown on other handlers
  ritr = post_shutdown_handlers_.rbegin();
  for ( ; ritr != post_shutdown_handlers_.rend(); ++ritr) {
    (*ritr)();
  }
  // call stop() to break out of the main serve() function
  server_->stop();
  LOG(INFO) << "Server stopped.";
}

}  // namespace common
