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

#include <sys/socket.h>
#include <csignal>
#include <chrono>
#include <string>

#include "folly/Baton.h"
#include "folly/io/async/AsyncSignalHandler.h"
#include "gtest/gtest.h"

#include "common/graceful_shutdown_handler.h"
#include "common/timer.h"
#include "common/tests/thrift/gen-cpp2/DummyService.h"
#include "common/thrift_client_pool.h"

using apache::thrift::HandlerCallback;
using apache::thrift::RpcOptions;
using apache::thrift::transport::TTransportException;
using apache::thrift::TApplicationException;
using common::ThriftClientPool;
using dummy_service::thrift::DummyServiceSvIf;
using dummy_service::thrift::DummyServiceAsyncClient;
using folly::AsyncSignalHandler;
using folly::AsyncSocket;
using folly::EventBaseManager;
using folly::makeFuture;
using folly::Unit;
using std::chrono::milliseconds;
using std::chrono::seconds;

namespace common {

struct LatchServiceHandler : public DummyServiceSvIf {
  void async_tm_ping(
      std::unique_ptr<apache::thrift::HandlerCallback<void>> callback)
    override {
    callback->done();
  }

  void async_tm_getSomething(
      std::unique_ptr<apache::thrift::HandlerCallback<int64_t>> callback,
      int64_t input) override {
    baton_.wait();
    callback->result(input);
  }

  folly::Baton<> baton_;
};

class GracefulShutdownTest : public ::testing::Test {
 protected:
  void SetUp() override {
    handler_ = std::make_shared<LatchServiceHandler>();
    server_ = std::make_shared<apache::thrift::ThriftServer>();
    server_->setUseClientTimeout(true);
    server_->setPort(port_);
    server_->setInterface(handler_);
  }

  std::string host_ = "127.0.0.1";
  int port_ = 59090;
  std::shared_ptr<LatchServiceHandler> handler_;
  std::shared_ptr<apache::thrift::ThriftServer> server_;
};

TEST_F(GracefulShutdownTest, OneOutstandingRequestTest) {
  // starts server
  std::thread server_thread([this] {
      GracefulShutdownHandler gracefulShutdown(server_);
      gracefulShutdown.RegisterShutdownSignal(SIGUSR1);
      server_->serve();
  });
  sleep(1);

  ThriftClientPool<DummyServiceAsyncClient> client_pool;
  auto client = client_pool.getClient(server_->getAddress());

  RpcOptions options;
  options.setTimeout(seconds(5));
  // send a request before shutdown
  auto successful_result = client->future_getSomething(options, 123);
  sleep(1);
  // signal shutdown
  std::raise(SIGUSR1);
  sleep(1);
  // now allow the request to finish
  handler_->baton_.post();
  EXPECT_EQ(successful_result.get(), 123);

  // make sure server is shut down
  server_thread.join();

  // send a request after shutdown
  auto failure_result = client->future_getSomething(options, 456);
  EXPECT_THROW(failure_result.get(), TTransportException);
}

TEST_F(GracefulShutdownTest, TwoOutstandingRequestTest) {
  // starts server
  std::thread server_thread([this] {
      GracefulShutdownHandler gracefulShutdown(server_);
      gracefulShutdown.RegisterShutdownSignal(SIGUSR1);
      server_->setNPoolThreads(1);
      server_->serve();
  });
  sleep(1);

  ThriftClientPool<DummyServiceAsyncClient> client_pool;
  auto client = client_pool.getClient(server_->getAddress());

  RpcOptions options;
  options.setTimeout(seconds(5));
  // send a request before shutdown
  auto successful_result = client->future_getSomething(options, 123);
  auto successful_result2 = client->future_getSomething(options, 456);
  sleep(1);
  // signal shutdown
  std::raise(SIGUSR1);
  sleep(1);
  // now allow the request to finish
  handler_->baton_.post();
  EXPECT_EQ(successful_result.get(), 123);
  EXPECT_EQ(successful_result2.get(), 456);

  // make sure server is shut down
  server_thread.join();
}

TEST_F(GracefulShutdownTest, NewRequestAfterPostShutdownTest) {
  // starts server
  std::thread server_thread([this] {
      GracefulShutdownHandler gracefulShutdown(server_);
      gracefulShutdown.RegisterShutdownSignal(SIGUSR1);
      server_->serve();
  });
  sleep(1);

  ThriftClientPool<DummyServiceAsyncClient> client_pool;
  auto client = client_pool.getClient(server_->getAddress());
  RpcOptions options;
  options.setTimeout(seconds(5));
  auto successful_result = client->future_getSomething(options, 123);
  sleep(1);
  // signal shutdown
  std::raise(SIGUSR1);
  sleep(1);
  EXPECT_THROW(client->future_getSomething(options, 456).get(),
               std::exception);

  handler_->baton_.post();
  EXPECT_EQ(successful_result.get(), 123);
  server_thread.join();
}

TEST_F(GracefulShutdownTest, NewConnectionAfterPostShutdownTest) {
  // starts server
  std::thread server_thread([this] {
      GracefulShutdownHandler gracefulShutdown(server_);
      gracefulShutdown.RegisterShutdownSignal(SIGUSR1);
      server_->serve();
  });
  sleep(1);

  ThriftClientPool<DummyServiceAsyncClient> client_pool;
  auto client = client_pool.getClient(server_->getAddress());
  RpcOptions options;
  options.setTimeout(seconds(5));
  auto successful_result = client->future_getSomething(options, 123);

  // signal shutdown
  std::raise(SIGUSR1);
  sleep(1);

  struct ConnCallback : public AsyncSocket::ConnectCallback {
    explicit ConnCallback()
      : exception_(folly::AsyncSocketException::UNKNOWN, "none") {}
    void connectSuccess() noexcept override {}
    void connectErr(const folly::AsyncSocketException& ex) noexcept override {
      exception_ = ex;
    }
    folly::AsyncSocketException exception_;
  };
  folly::EventBase evb;
  ConnCallback cb;
  auto socket = AsyncSocket::newSocket(&evb);
  socket->connect(&cb, server_->getAddress(), 30);
  evb.loop();
  EXPECT_EQ(cb.exception_.getType(), folly::AsyncSocketException::NOT_OPEN);

  handler_->baton_.post();
  EXPECT_EQ(successful_result.get(), 123);
  server_thread.join();
}


}  // namespace common


int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
