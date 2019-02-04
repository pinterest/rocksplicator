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
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "gtest/gtest.h"
#include "common/tests/thrift/gen-cpp2/DummyService.h"
#include "common/thrift_client_pool.h"
#include "thrift/lib/cpp2/server/ThriftServer.h"

using apache::thrift::HandlerCallback;
using apache::thrift::ThriftServer;
using apache::thrift::transport::TTransportException;

using common::ThriftClientPool;

using folly::Future;
using folly::Try;
using folly::Unit;

using dummy_service::thrift::DummyServiceAsyncClient;
using dummy_service::thrift::DummyServiceErrorCode;
using dummy_service::thrift::DummyServiceException;
using dummy_service::thrift::DummyServiceSvIf;
using apache::thrift::RpcOptions;

using std::atomic;
using std::chrono::milliseconds;
using std::make_shared;
using std::make_tuple;
using std::make_unique;
using std::move;
using std::shared_ptr;
using std::thread;
using std::this_thread::sleep_for;
using std::string;
using std::tie;
using std::tuple;
using std::unique_ptr;
using std::vector;

static const char* gLocalIp = "127.0.0.1";
static const uint16_t gPort = 9090;

class DummyServiceTestHandler : public DummyServiceSvIf {
 public:
  explicit DummyServiceTestHandler(const uint32_t delayMs) :
      delayMs_(delayMs), nPings_(0) {
  }

  void async_tm_ping(unique_ptr<HandlerCallback<void>> callback)
      override {
    ++nPings_;
    auto delayMs = delayMs_.load();
    if (delayMs > 0) {
      sleep_for(milliseconds(delayMs));
    }

    callback->done();
  }

  void async_tm_getSomething(
    unique_ptr<HandlerCallback<int64_t>> callback,
    int64_t input) override {
    DummyServiceException e;
    e.errorCode = DummyServiceErrorCode::DUMMY_ERROR;
    e.message = "Intended exception";
    callback.release()->exceptionInThread(e);
  }

  atomic<uint32_t> delayMs_;
  atomic<uint32_t> nPings_;
};

tuple<shared_ptr<DummyServiceTestHandler>,
      shared_ptr<ThriftServer>,
      unique_ptr<thread>>
makeServer(uint16_t port, uint32_t delayMs) {
  auto handler = make_shared<DummyServiceTestHandler>(delayMs);
  auto server = make_shared<ThriftServer>();
  server->setPort(port);
  server->setInterface(handler);
  auto t = make_unique<thread>([server, port] {
      LOG(INFO) << "Start server on port " << port;
      server->serve();
      LOG(INFO) << "Exit server on port " << port;
    });

  return make_tuple(handler, server, move(t));
}

void testBasics(ThriftClientPool<DummyServiceAsyncClient>* pool) {
  const std::atomic<bool>* is_good;
  LOG(ERROR) << " Here ";
  auto client = pool->getClient(gLocalIp, gPort, 0, &is_good);

  EXPECT_TRUE(client != nullptr);
  sleep_for(milliseconds(100));
  EXPECT_FALSE(is_good->load());

  // Server is not available
  EXPECT_THROW(client->future_ping().get(), TTransportException);
  EXPECT_THROW(client->future_ping().get(), TTransportException);

  LOG(ERROR) << " Here ";
  // re-get client, server is still not available
  LOG(ERROR) << " Here ";

  client = pool->getClient(gLocalIp, gPort, 0, &is_good);
  LOG(ERROR) << " Here ";

  sleep_for(milliseconds(100));
  EXPECT_TRUE(client != nullptr);
  EXPECT_THROW(client->future_ping().get(), TTransportException);
  EXPECT_FALSE(is_good->load());
  LOG(ERROR) << " Here ";

  // start the server
  shared_ptr<DummyServiceTestHandler> handler;
  shared_ptr<ThriftServer> server;
  unique_ptr<thread> thr;
  tie(handler, server, thr) = makeServer(gPort, 0);
  sleep(1);

  // The old connection should not work
  EXPECT_THROW(client->future_ping().get(), TTransportException);
  EXPECT_FALSE(is_good->load());
  EXPECT_EQ(handler->nPings_.load(), 0);

  // create a new connection, and it should work now
  LOG(ERROR) << " Here ";

  client = pool->getClient(gLocalIp, gPort, 0, &is_good);
  LOG(ERROR) << " Here ";

  sleep_for(milliseconds(100));
  EXPECT_TRUE(client != nullptr);
  EXPECT_TRUE(is_good->load());
  EXPECT_NO_THROW(client->future_ping().get());
  EXPECT_EQ(handler->nPings_.load(), 1);

  // create a new connection again, and it should work
  LOG(ERROR) << " Here ";

  client = pool->getClient(gLocalIp, gPort, 0, &is_good);

  LOG(ERROR) << " Here ";

  sleep_for(milliseconds(100));
  EXPECT_TRUE(client != nullptr);
  EXPECT_TRUE(is_good->load());
  EXPECT_NO_THROW(client->future_ping().get());
  EXPECT_EQ(handler->nPings_.load(), 2);
  EXPECT_TRUE(is_good->load());

  // test exception
  EXPECT_THROW(client->future_getSomething(1).get(),
               DummyServiceException);

  // client is still good after an exception
  EXPECT_NO_THROW(client->future_ping().get());
  EXPECT_TRUE(is_good->load());
  EXPECT_EQ(handler->nPings_.load(), 3);

  // 200 ms delay
  handler->delayMs_.store(200);
  // no timeout
  EXPECT_NO_THROW(client->future_ping().get());
  // 100 ms timeout
  {
    RpcOptions options;
    options.setTimeout(milliseconds(100));
    EXPECT_THROW(client->future_ping(options).get(), TTransportException);
    EXPECT_TRUE(is_good->load());
  }

  // client is good
  EXPECT_NO_THROW(client->future_ping().get());
  EXPECT_TRUE(is_good->load());

  // 300 ms timeout
  {
    RpcOptions options;
    options.setTimeout(milliseconds(300));
    EXPECT_NO_THROW(client->future_ping(options).get());
    EXPECT_TRUE(is_good->load());
    EXPECT_EQ(handler->nPings_.load(), 7);
  }

  // stop the server
  server->stop();
  sleep(1);

  // exception
  {
    RpcOptions options;
    options.setTimeout(milliseconds(300));
    EXPECT_FALSE(is_good->load());
    EXPECT_THROW(client->future_ping(options).get(), TTransportException);
    EXPECT_THROW(client->future_ping().get(), TTransportException);
    EXPECT_FALSE(is_good->load());
    EXPECT_EQ(handler->nPings_.load(), 7);
  }

  // create a new client, and it should not work
  LOG(ERROR) << " Here ";

  client = pool->getClient(gLocalIp, gPort, 0, &is_good);

  LOG(ERROR) << " Here ";

  sleep_for(milliseconds(100));
  EXPECT_THROW(client->future_ping().get(), TTransportException);
  EXPECT_FALSE(is_good->load());

  thr->join();
}

TEST(ThriftClientTest, Basics) {
  ThriftClientPool<DummyServiceAsyncClient> pool_default;
  LOG(ERROR) << "here";
  testBasics(&pool_default);
  LOG(ERROR) << "here";

  auto pool_default_shared_1 =
    pool_default.shareIOThreads<DummyServiceAsyncClient>();
  LOG(ERROR) << "here";

  testBasics(pool_default_shared_1.get());
  LOG(ERROR) << "here";

  auto pool_default_shared_2 =
    pool_default_shared_1->shareIOThreads<DummyServiceAsyncClient>();
  LOG(ERROR) << "here";
  testBasics(pool_default_shared_2.get());
  LOG(ERROR) << "here";

  ThriftClientPool<DummyServiceAsyncClient> pool_1(1);
  LOG(ERROR) << "here";
  testBasics(&pool_1);

  LOG(ERROR) << "here";
  ThriftClientPool<DummyServiceAsyncClient> pool_100(100);
  LOG(ERROR) << "here";
  testBasics(&pool_100);
}

void stressTest(uint32_t nThreads, uint32_t nCalls, uint32_t nBatchSz,
                ThriftClientPool<DummyServiceAsyncClient>* pool) {
  LOG(INFO) << nThreads << " nThreads; "
            << nCalls << " nCalls; "
            << nBatchSz << " nBatchSz";
  vector<thread> threads(nThreads);
  auto shared_pool = pool->shareIOThreads<DummyServiceAsyncClient>();
  ThriftClientPool<DummyServiceAsyncClient>* selected_pool = nullptr;
  for (uint32_t i = 0 ; i < nThreads; ++i) {
    selected_pool = i % 2 == 1 ? pool : shared_pool.get();
    threads[i] = thread([nCalls, nBatchSz, pool = selected_pool] () {
        auto client = pool->getClient(gLocalIp, gPort);
        EXPECT_TRUE(client != nullptr);
        auto nRounds = nCalls / nBatchSz;
        for (uint32_t j = 0; j < nRounds; ++j) {
          vector<Future<Unit>> v;
          for (uint32_t k = 0; k < nBatchSz; ++k) {
            v.push_back(client->future_ping());
          }

          for (auto& f : v) {
            EXPECT_NO_THROW(f.get());
          }
        }
      });
  }

  for (auto& thr : threads) {
    thr.join();
  }
}


TEST(ThriftClientTest, Stress) {
  shared_ptr<DummyServiceTestHandler> handler;
  shared_ptr<ThriftServer> server;
  unique_ptr<thread> thr;
  tie(handler, server, thr) = makeServer(gPort, 0);
  sleep(1);

  ThriftClientPool<DummyServiceAsyncClient> pool_1(1);
  stressTest(100, 1000, 1, &pool_1);
  stressTest(100, 1000, 100, &pool_1);
  stressTest(100, 1000, 500, &pool_1);

  ThriftClientPool<DummyServiceAsyncClient> pool_100(100);
  stressTest(100, 1000, 1, &pool_100);
  stressTest(100, 1000, 100, &pool_100);
  stressTest(100, 1000, 500, &pool_100);

  EXPECT_EQ(handler->nPings_.load(), 100 * 1000 * 6);

  server->stop();
  thr->join();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_channel_cleanup_min_interval_seconds = -1;
  return RUN_ALL_TESTS();
}
