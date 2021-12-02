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
#include <fstream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "common/tests/thrift/gen-cpp2/DummyService.h"
#include "common/thrift_router.h"
#include "thrift/lib/cpp2/server/ThriftServer.h"

using apache::thrift::HandlerCallback;
using apache::thrift::ThriftServer;
using apache::thrift::transport::TTransportException;

using folly::split;

using std::atoi;
using std::atomic;
using std::make_shared;
using std::make_tuple;
using std::make_unique;
using std::move;
using std::ofstream;
using std::string;
using std::shared_ptr;
using std::thread;
using std::tie;
using std::tuple;
using std::unique_ptr;
using std::vector;

using common::ThriftRouter;

using dummy_service::thrift::DummyServiceAsyncClient;
using dummy_service::thrift::DummyServiceSvIf;

static const char* g_config_path = "./thrift_router_test_config_file";
static const char* g_config_v1 =
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

static const char* g_config_v1az =
  "{"
  "  \"user_pins\": {"
  "  \"num_leaf_segments\": 3,"
  "  \"127.0.0.1:8090:us-east-1a\": [\"00000\", \"00001\", \"00002\"],"
  "  \"127.0.0.1:8091:us-east-1c\": [\"00000\", \"00001\", \"00002\"],"
  "  \"127.0.0.1:8092:us-east-1e\": [\"00000\", \"00001\", \"00002\"]"
  "   }"
  "}";

static const char* g_config_v2 =
  "{"
  "  \"user_pins\": {"
  "  \"num_leaf_segments\": 3,"
  "  \"127.0.0.1:8090:zone_a\": [\"00002:M\", \"00000:M\"],"
  "  \"127.0.0.1:8091:test\": [\"00000:S\", \"00001:M\", \"00002:M\"],"
  "  \"127.0.0.1:8092:zone_c\": [\"00001:S\", \"00002:M\"]"
  "   },"
  "  \"interest_pins\": {"
  "  \"num_leaf_segments\": 2,"
  "  \"127.0.0.1:8090:zone_a\": [\"00000:M\"],"
  "  \"127.0.0.1:8091:test\": [\"00001:S\"]"
  "   }"
  "}";

static const char* g_config_v3 =
  "{"
  "  \"user_pins\": {"
  "  \"num_leaf_segments\": 3,"
  "  \"127.0.0.1:8090:us-east-1a\": [\"00000:S\", \"00001:S\", \"00002:M\"],"
  "  \"127.0.0.1:8091:us-east-1c\": [\"00000:S\", \"00001:M\", \"00002:S\"],"
  "  \"127.0.0.1:8092:us-east-1e\": [\"00000:M\", \"00001:S\", \"00002:S\"]"
  "   }"
  "}";

// This config has 2 groups under each zone
static const char* g_config_v4 =
  "{"
  "  \"user_pins\": {"
  "  \"num_leaf_segments\": 4,"
  "  \"127.0.0.1:8090:us-east-1a_0\": [\"00000:M\", \"00001:S\", \"00002:S\", \"00003:S\"],"
  "  \"127.0.0.1:8091:us-east-1a_1\": [\"00000:S\", \"00001:M\", \"00002:S\", \"00003:S\"],"
  "  \"127.0.0.1:8092:us-east-1b_0\": [\"00000:S\", \"00001:S\", \"00002:M\", \"00003:S\"],"
  "  \"127.0.0.1:8093:us-east-1b_1\": [\"00000:S\", \"00001:S\", \"00002:S\", \"00003:M\"]"
  "   }"
  "}";

// This config contains an unreachable host
static const char* g_config_v5 =
  "{"
  "  \"user_pins\": {"
  "  \"num_leaf_segments\": 1,"
  "  \"10.255.255.1:80\": [\"00000:M\"]"
  "   }"
  "}";

using ClusterLayout = ThriftRouter<DummyServiceAsyncClient>::ClusterLayout;
using Role = ThriftRouter<DummyServiceAsyncClient>::Role;
using Quantity = ThriftRouter<DummyServiceAsyncClient>::Quantity;
using ReturnCode = ThriftRouter<DummyServiceAsyncClient>::ReturnCode;
using Host = ThriftRouter<DummyServiceAsyncClient>::Host;

struct DummyServiceTestHandler : public DummyServiceSvIf {
 public:
  DummyServiceTestHandler() : nPings_(0) {
  }

  void async_tm_ping(unique_ptr<HandlerCallback<void>> callback) override {
    ++nPings_;
    callback->done();
  }

  atomic<uint32_t> nPings_;
};

tuple<shared_ptr<DummyServiceTestHandler>,
      shared_ptr<ThriftServer>,
      unique_ptr<thread>>
makeServer(uint16_t port) {
  auto handler = make_shared<DummyServiceTestHandler>();
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

void updateConfigFile(const string& content) {
  ofstream os(g_config_path);
  EXPECT_TRUE(os);
  os << content;
}

TEST(ThriftRouterTest, Basics) {
  updateConfigFile("");
  ThriftRouter<DummyServiceAsyncClient> router(
    "test", g_config_path, common::parseConfig);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 0);
  EXPECT_EQ(router.getShardNumberFor("interest_pins"), 0);
  EXPECT_EQ(router.getShardNumberFor("unknown"), 0);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 0), 0);
  EXPECT_EQ(router.getHostNumberFor("interest_pins", 1), 0);
  EXPECT_EQ(router.getHostNumberFor("unknown", 0), 0);

  std::vector<shared_ptr<DummyServiceAsyncClient>> v;
  EXPECT_EQ(
    router.getClientsFor("unknown_segment", Role::ANY, Quantity::ONE, 1, &v),
    ReturnCode::UNKNOWN_SEGMENT);

  shared_ptr<DummyServiceTestHandler> handlers[3];
  shared_ptr<ThriftServer> servers[3];
  unique_ptr<thread> thrs[3];

  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  tie(handlers[1], servers[1], thrs[1]) = makeServer(8091);
  tie(handlers[2], servers[2], thrs[2]) = makeServer(8092);
  updateConfigFile(g_config_v1);
  sleep(1);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 3);
  EXPECT_EQ(router.getShardNumberFor("interest_pins"), 2);
  EXPECT_EQ(router.getShardNumberFor("unknown"), 0);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 0), 1);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 1), 1);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 2), 3);
  EXPECT_EQ(router.getShardNumberFor("interest_pins"), 2);
  EXPECT_EQ(router.getHostNumberFor("interest_pins", 0), 1);
  EXPECT_EQ(router.getHostNumberFor("interest_pins", 1), 1);
  EXPECT_EQ(router.getShardNumberFor("unknown"), 0);
  EXPECT_EQ(router.getHostNumberFor("unknown", 1), 0);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 3);
  for (auto client : v) {
    EXPECT_NO_THROW(client->future_ping().get());
  }
  for (const auto& h : handlers) {
    EXPECT_EQ(h->nPings_.load(), 1);
  }

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::MASTER, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 3);
  for (auto client : v) {
    EXPECT_NO_THROW(client->future_ping().get());
  }
  for (const auto& h : handlers) {
    EXPECT_EQ(h->nPings_.load(), 2);
  }

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::SLAVE, Quantity::ALL, 2, &v),
    ReturnCode::NOT_FOUND);


  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::MASTER, Quantity::ONE, 0, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 1);
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[0]->nPings_.load(), 3);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, 0, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 1);
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[0]->nPings_.load(), 4);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::TWO, 2, &v),
    ReturnCode::OK);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 3);

  EXPECT_EQ(
    router.getClientsFor("interest_pins", Role::ANY, Quantity::ONE, 2, &v),
    ReturnCode::UNKNOWN_SHARD);

  EXPECT_EQ(
    router.getClientsFor("interest_pins", Role::ANY, Quantity::ALL, 0, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 1);
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[0]->nPings_.load(), 5);
  // stop servers[0]
  servers[0]->stop();
  thrs[0]->join();

  EXPECT_THROW(v[0]->future_ping().get(), TTransportException);
  EXPECT_EQ(handlers[0]->nPings_.load(), 5);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::MASTER, Quantity::ONE, 0, &v),
    ReturnCode::BAD_HOST);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::MASTER, Quantity::ALL, 2, &v),
    ReturnCode::BAD_HOST);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, 0, &v),
    ReturnCode::BAD_HOST);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::BAD_HOST);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::MASTER, Quantity::TWO, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 2);
  for (auto client : v) {
    EXPECT_NO_THROW(client->future_ping().get());
  }
  EXPECT_EQ(handlers[0]->nPings_.load(), 5);
  EXPECT_EQ(handlers[1]->nPings_.load(), 3);
  EXPECT_EQ(handlers[2]->nPings_.load(), 3);

  // change config file
  updateConfigFile(g_config_v2);
  sleep(1);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 3);
  EXPECT_EQ(router.getShardNumberFor("interest_pins"), 2);
  EXPECT_EQ(router.getShardNumberFor("unknown"), 0);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 0), 2);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 1), 2);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 2), 3);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::SLAVE, Quantity::ALL, 1, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 1);
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[2]->nPings_.load(), 4);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 1, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 2);
  EXPECT_NO_THROW(v[0]->future_ping().get());  // Master should be the first.
  EXPECT_EQ(handlers[1]->nPings_.load(), 4);
  EXPECT_NO_THROW(v[1]->future_ping().get()); // Slave should be the second
  EXPECT_EQ(handlers[2]->nPings_.load(), 5);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::MASTER, Quantity::TWO, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 2);
  // v[0] must be 9091, as it is local
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[1]->nPings_.load(), 5);

  // restart servers[0]
  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::MASTER, Quantity::ALL, 0, &v),
    ReturnCode::BAD_HOST);

  sleep(4);
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::MASTER, Quantity::ALL, 0, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 1);
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[0]->nPings_.load(), 1);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 3);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::TWO, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 2);

  // stop all servers
  for (auto& s : servers) {
    s->stop();
  }

  for (auto& t : thrs) {
    t->join();
  }
}

void stress(int n_threads, int n_ops) {
  vector<thread> threads(n_threads);
  shared_ptr<DummyServiceTestHandler> handlers[3];
  shared_ptr<ThriftServer> servers[3];
  unique_ptr<thread> thrs[3];
  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  tie(handlers[1], servers[1], thrs[1]) = makeServer(8091);
  tie(handlers[2], servers[2], thrs[2]) = makeServer(8092);

  updateConfigFile(g_config_v2);
  ThriftRouter<DummyServiceAsyncClient> router(
    "", g_config_path, common::parseConfig);
  sleep(1);

  for (int i = 0; i < n_threads; ++i) {
    threads[i] = thread([&router, n_ops] () {
        std::vector<shared_ptr<DummyServiceAsyncClient>> v;
        for (int j = 0; j < n_ops; ++j) {
          EXPECT_EQ(
            router.getClientsFor("user_pins", Role::MASTER, Quantity::ALL, 0,
                                 &v),
            ReturnCode::OK);
          EXPECT_EQ(v.size(), 1);
          EXPECT_NO_THROW(v[0]->future_ping().get());

          EXPECT_EQ(
            router.getClientsFor("user_pins", Role::MASTER, Quantity::ALL, 1,
                                 &v),
            ReturnCode::OK);
          EXPECT_EQ(v.size(), 1);
          EXPECT_NO_THROW(v[0]->future_ping().get());

          EXPECT_EQ(
            router.getClientsFor("user_pins", Role::SLAVE, Quantity::ALL, 1,
                                 &v),
            ReturnCode::OK);
          EXPECT_EQ(v.size(), 1);
          EXPECT_NO_THROW(v[0]->future_ping().get());

          EXPECT_EQ(
            router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 1,
                                 &v),
            ReturnCode::OK);
          EXPECT_EQ(v.size(), 2);
        }
      });
  }

  for (auto& t : threads) {
    t.join();
  }

  for (auto& h : handlers) {
    EXPECT_EQ(h->nPings_.load(), n_threads * n_ops);
  }

  // stop all servers
  for (auto& s : servers) {
    s->stop();
  }

  for (auto& t : thrs) {
    t->join();
  }
}

TEST(ThriftRouterTest, Stress) {
  stress(1, 1000);
  stress(99, 1000);
}

TEST(ThriftRouterTest, LocalAzTest) {
  updateConfigFile(g_config_v1az);
  ThriftRouter<DummyServiceAsyncClient> router(
    "us-east-1a", g_config_path, common::parseConfig);

  std::vector<shared_ptr<DummyServiceAsyncClient>> v;
  shared_ptr<DummyServiceTestHandler> handlers[3];
  shared_ptr<ThriftServer> servers[3];
  unique_ptr<thread> thrs[3];

  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  tie(handlers[1], servers[1], thrs[1]) = makeServer(8091);
  tie(handlers[2], servers[2], thrs[2]) = makeServer(8092);
  sleep(1);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 0), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 1), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 2), 3);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 3);
  for (auto client : v) {
    EXPECT_NO_THROW(client->future_ping().get());
  }
  for (const auto& h : handlers) {
    EXPECT_EQ(h->nPings_.load(), 1);
  }

  // Get the client from local az
  // All requests should hit the local az handler
  for (int i = 0; i < 100; i ++) {
    std::vector<shared_ptr<DummyServiceAsyncClient>> v;
    EXPECT_EQ(
        router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, 2, &v),
        ReturnCode::OK);
    EXPECT_EQ(v.size(), 1);
    v[0]->future_ping().get();
  }
  EXPECT_EQ(handlers[0]->nPings_.load(), 101);

  // stop all servers
  for (auto& s : servers) {
    s->stop();
  }

  for (auto& t : thrs) {
    t->join();
  }
}

TEST(ThriftRouterTest, SpecificAzTest) {
  FLAGS_always_prefer_local_host = true;
  updateConfigFile(g_config_v1az);
  ThriftRouter<DummyServiceAsyncClient> router(
    "us-east-1a", g_config_path, common::parseConfig);

  std::vector<shared_ptr<DummyServiceAsyncClient>> v;
  shared_ptr<DummyServiceTestHandler> handlers[3];
  shared_ptr<ThriftServer> servers[3];
  unique_ptr<thread> thrs[3];

  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  tie(handlers[1], servers[1], thrs[1]) = makeServer(8091);
  tie(handlers[2], servers[2], thrs[2]) = makeServer(8092);
  sleep(1);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 0), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 1), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 2), 3);

  // no hosts in the az specified
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v, "us-east-1f"),
    ReturnCode::NOT_FOUND);
  EXPECT_EQ(v.size(), 0);

  // code works without az specified
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 3);
  for (auto client : v) {
    EXPECT_NO_THROW(client->future_ping().get());
  }
  for (const auto& h : handlers) {
    EXPECT_EQ(h->nPings_.load(), 1);
  }

  // Get the client from specified az for specific shard
  for (int i = 0; i < 100; i ++) {
    v.clear();
    EXPECT_EQ(
        router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v, "us-east-1c"),
        ReturnCode::OK);
    EXPECT_EQ(v.size(), 1);
    v[0]->future_ping().get();
  }
  EXPECT_EQ(handlers[1]->nPings_.load(), 101);

  // Get the client from specified az
  for (int i = 0; i < 100; i ++) {
    std::map<uint32_t, std::vector<std::shared_ptr<DummyServiceAsyncClient>>> m;
    m[2];
    EXPECT_EQ(
        router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, &m, "us-east-1c"),
        ReturnCode::OK);
    EXPECT_EQ(m.size(), 1);
    EXPECT_EQ(m[2].size(), 1);
    m[2][0]->future_ping().get();
  }
  EXPECT_EQ(handlers[1]->nPings_.load(), 201);

  // stop all servers
  for (auto& s : servers) {
    s->stop();
  }

  for (auto& t : thrs) {
    t->join();
  }
  FLAGS_always_prefer_local_host = false;
}

TEST(ThriftRouterTest, SpecificAzMasterSlaveTest) {
  updateConfigFile(g_config_v4);
  ThriftRouter<DummyServiceAsyncClient> router(
    "us-east-1a", g_config_path, common::parseConfig);

  std::vector<shared_ptr<DummyServiceAsyncClient>> v;
  shared_ptr<DummyServiceTestHandler> handlers[4];
  shared_ptr<ThriftServer> servers[4];
  unique_ptr<thread> thrs[4];

  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  tie(handlers[1], servers[1], thrs[1]) = makeServer(8091);
  tie(handlers[2], servers[2], thrs[2]) = makeServer(8092);
  tie(handlers[3], servers[3], thrs[3]) = makeServer(8093);
  sleep(1);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 0), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 1), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 2), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 3), 4);

  // no hosts in the az specified
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v, "us-east-1f"),
    ReturnCode::NOT_FOUND);
  EXPECT_EQ(v.size(), 0);

  // Get the client from local az for specific shard
  // All requests should hit the local az handler
  for (int i = 0; i < 100; i ++) {
    v.clear();
    EXPECT_EQ(
        router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, 2, &v, "us-east-1b"),
        ReturnCode::OK);
    EXPECT_EQ(v.size(), 1);
    v[0]->future_ping().get();
  }
  EXPECT_EQ(handlers[2]->nPings_.load(), 100);

  // Get the client from host which is slave in the specified az
  for (int i = 0; i < 100; i ++) {
    v.clear();
    EXPECT_EQ(
        router.getClientsFor("user_pins", Role::SLAVE, Quantity::ONE, 2, &v, "us-east-1b"),
        ReturnCode::OK);
    EXPECT_EQ(v.size(), 1);
    v[0]->future_ping().get();
  }
  EXPECT_EQ(handlers[3]->nPings_.load(), 100);

  // Get the client from host which is master in the specified az
  for (int i = 0; i < 100; i ++) {
    v.clear();
    EXPECT_EQ(
        router.getClientsFor("user_pins", Role::MASTER, Quantity::ONE, 2, &v, "us-east-1b"),
        ReturnCode::OK);
    EXPECT_EQ(v.size(), 1);
    v[0]->future_ping().get();
  }
  EXPECT_EQ(handlers[2]->nPings_.load(), 200);

  // no hosts with the role in the az specified
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::MASTER, Quantity::ONE, 2, &v, "us-east-1a"),
    ReturnCode::NOT_FOUND);
  EXPECT_EQ(v.size(), 0);

  // stop all servers
  for (auto& s : servers) {
    s->stop();
  }

  for (auto& t : thrs) {
    t->join();
  }
}

TEST(ThriftRouterTest, ForeignAzTest) {
  updateConfigFile(g_config_v1az);
  ThriftRouter<DummyServiceAsyncClient> router(
    "us-east-1b", g_config_path, common::parseConfig);

  std::vector<shared_ptr<DummyServiceAsyncClient>> v;
  shared_ptr<DummyServiceTestHandler> handlers[3];
  shared_ptr<ThriftServer> servers[3];
  unique_ptr<thread> thrs[3];

  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  tie(handlers[1], servers[1], thrs[1]) = makeServer(8091);
  tie(handlers[2], servers[2], thrs[2]) = makeServer(8092);
  sleep(1);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 0), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 1), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 2), 3);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 3);
  for (auto client : v) {
    EXPECT_NO_THROW(client->future_ping().get());
  }
  for (const auto& h : handlers) {
    EXPECT_EQ(h->nPings_.load(), 1);
  }

  for (int i = 0; i < 100; i ++) {
    std::vector<shared_ptr<DummyServiceAsyncClient>> v;
    EXPECT_EQ(
        router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, 2, &v),
        ReturnCode::OK);
    EXPECT_EQ(v.size(), 1);
    v[0]->future_ping().get();
  }
  ASSERT_TRUE(handlers[0]->nPings_.load() > 1);
  ASSERT_TRUE(handlers[1]->nPings_.load() > 1);
  ASSERT_TRUE(handlers[2]->nPings_.load() > 1);

  // stop all servers
  for (auto& s : servers) {
    s->stop();
  }

  for (auto& t : thrs) {
    t->join();
  }
}

TEST(ThriftRouterTest, ForeignAzMultiClientsTest) {
  updateConfigFile(g_config_v1az);
  ThriftRouter<DummyServiceAsyncClient> router(
    "us-east-1b", g_config_path, common::parseConfig);

  shared_ptr<DummyServiceTestHandler> handlers[3];
  shared_ptr<ThriftServer> servers[3];
  unique_ptr<thread> thrs[3];

  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  tie(handlers[1], servers[1], thrs[1]) = makeServer(8091);
  tie(handlers[2], servers[2], thrs[2]) = makeServer(8092);
  sleep(1);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 3);

  // Definitive routing test.
  int previous_ping_count[3] = {0, 0, 0};
  for (int i = 0; i < 100; i ++) {
    std::map<uint32_t, std::vector<std::shared_ptr<DummyServiceAsyncClient>>> v;
    v[0]; v[1]; v[2];
    EXPECT_EQ(
        router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, &v),
        ReturnCode::OK);
    v[0][0]->future_ping().get();
    v[1][0]->future_ping().get();
    v[2][0]->future_ping().get();
    for (int j = 0; j < 3; j ++) {
      if (handlers[j]->nPings_.load() != previous_ping_count[j]) {
        ASSERT_TRUE(handlers[j]->nPings_.load() - previous_ping_count[j] == 3);
        previous_ping_count[j] = handlers[j]->nPings_.load();
      }
    }
  }
  EXPECT_EQ(previous_ping_count[0]
            + previous_ping_count[1]
            + previous_ping_count[2], 300);

  // stop all servers
  for (auto& s : servers) {
    s->stop();
  }

  for (auto& t : thrs) {
    t->join();
  }
}

TEST(ThriftRouterTest, HostOrderTest) {
  updateConfigFile(g_config_v3);
  ThriftRouter<DummyServiceAsyncClient> router(
    "us-east-1c", g_config_path, common::parseConfig);

  std::vector<shared_ptr<DummyServiceAsyncClient>> v;
  shared_ptr<DummyServiceTestHandler> handlers[3];
  shared_ptr<ThriftServer> servers[3];
  unique_ptr<thread> thrs[3];

  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  tie(handlers[1], servers[1], thrs[1]) = makeServer(8091);
  tie(handlers[2], servers[2], thrs[2]) = makeServer(8092);
  sleep(1);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 3);

  EXPECT_EQ(handlers[0]->nPings_.load(), 0);
  EXPECT_EQ(handlers[1]->nPings_.load(), 0);
  EXPECT_EQ(handlers[2]->nPings_.load(), 0);
  // ANY, ALL
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 3);
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[0]->nPings_.load(), 1);

  EXPECT_NO_THROW(v[1]->future_ping().get());
  EXPECT_EQ(handlers[1]->nPings_.load(), 1);

  EXPECT_NO_THROW(v[2]->future_ping().get());
  EXPECT_EQ(handlers[2]->nPings_.load(), 1);

  // MASTER, ALL
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::MASTER, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 1);
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[0]->nPings_.load(), 2);

  // SLAVE, ALL
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::SLAVE, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 2);
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[1]->nPings_.load(), 2);

  EXPECT_NO_THROW(v[1]->future_ping().get());
  EXPECT_EQ(handlers[2]->nPings_.load(), 2);


  // Set FLAGS_always_prefer_local_host
  FLAGS_always_prefer_local_host = true;

  EXPECT_EQ(handlers[0]->nPings_.load(), 2);
  EXPECT_EQ(handlers[1]->nPings_.load(), 2);
  EXPECT_EQ(handlers[2]->nPings_.load(), 2);
  // ANY, ALL
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 3);
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[0]->nPings_.load(), 2);
  EXPECT_EQ(handlers[1]->nPings_.load(), 3);
  EXPECT_EQ(handlers[2]->nPings_.load(), 2);

  EXPECT_NO_THROW(v[1]->future_ping().get());
  EXPECT_NO_THROW(v[2]->future_ping().get());
  EXPECT_EQ(handlers[0]->nPings_.load(), 3);
  EXPECT_EQ(handlers[1]->nPings_.load(), 3);
  EXPECT_EQ(handlers[2]->nPings_.load(), 3);

  // MASTER, ALL
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::MASTER, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 1);
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[0]->nPings_.load(), 4);

  // SLAVE, ALL
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::SLAVE, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 2);
  EXPECT_NO_THROW(v[0]->future_ping().get());
  EXPECT_EQ(handlers[1]->nPings_.load(), 4);

  EXPECT_NO_THROW(v[1]->future_ping().get());
  EXPECT_EQ(handlers[2]->nPings_.load(), 4);

  EXPECT_EQ(handlers[0]->nPings_.load(), 4);
  EXPECT_EQ(handlers[1]->nPings_.load(), 4);
  EXPECT_EQ(handlers[2]->nPings_.load(), 4);

  FLAGS_always_prefer_local_host = false;

  // stop all servers
  for (auto& s : servers) {
    s->stop();
  }

  for (auto& t : thrs) {
    t->join();
  }
}

TEST(ThriftRouterTest, ForeignHostGroupTestNonPreferLocal) {
  FLAGS_always_prefer_local_host = false;
  updateConfigFile(g_config_v4);
  ThriftRouter<DummyServiceAsyncClient> router(
    "us-east-1c", g_config_path, common::parseConfig);

  std::vector<shared_ptr<DummyServiceAsyncClient>> v;
  shared_ptr<DummyServiceTestHandler> handlers[4];
  shared_ptr<ThriftServer> servers[4];
  unique_ptr<thread> thrs[4];

  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  tie(handlers[1], servers[1], thrs[1]) = makeServer(8091);
  tie(handlers[2], servers[2], thrs[2]) = makeServer(8092);
  tie(handlers[3], servers[3], thrs[3]) = makeServer(8093);
  sleep(1);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 0), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 1), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 2), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 3), 4);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 4);
  for (auto client : v) {
    EXPECT_NO_THROW(client->future_ping().get());
  }
  for (const auto& h : handlers) {
    EXPECT_EQ(h->nPings_.load(), 1);
  }

  for (int i = 0; i < 100; i ++) {
    std::vector<shared_ptr<DummyServiceAsyncClient>> v;
    EXPECT_EQ(
        router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, 2, &v),
        ReturnCode::OK);
    EXPECT_EQ(v.size(), 1);
    v[0]->future_ping().get();
  }

  ASSERT_TRUE(handlers[0]->nPings_.load() == 1);
  ASSERT_TRUE(handlers[1]->nPings_.load() == 1);
  ASSERT_TRUE(handlers[2]->nPings_.load() == 101);
  ASSERT_TRUE(handlers[3]->nPings_.load() == 1);

  // stop all servers
  for (auto& s : servers) {
    s->stop();
  }

  for (auto& t : thrs) {
    t->join();
  }
}

TEST(ThriftRouterTest, ForeignHostGroupTestPreferLocal) {
  FLAGS_always_prefer_local_host = true;
  updateConfigFile(g_config_v4);
  ThriftRouter<DummyServiceAsyncClient> router(
    "us-east-1a", g_config_path, common::parseConfig);

  std::vector<shared_ptr<DummyServiceAsyncClient>> v;
  shared_ptr<DummyServiceTestHandler> handlers[4];
  shared_ptr<ThriftServer> servers[4];
  unique_ptr<thread> thrs[4];

  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  tie(handlers[1], servers[1], thrs[1]) = makeServer(8091);
  tie(handlers[2], servers[2], thrs[2]) = makeServer(8092);
  tie(handlers[3], servers[3], thrs[3]) = makeServer(8093);
  sleep(1);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 0), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 1), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 2), 4);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 3), 4);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 4);
  for (auto client : v) {
    EXPECT_NO_THROW(client->future_ping().get());
  }
  for (const auto& h : handlers) {
    EXPECT_EQ(h->nPings_.load(), 1);
  }

  for (int i = 0; i < 100; i ++) {
    std::vector<shared_ptr<DummyServiceAsyncClient>> v;
    EXPECT_EQ(
        router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, 2, &v),
        ReturnCode::OK);
    EXPECT_EQ(v.size(), 1);
    v[0]->future_ping().get();
  }
  ASSERT_TRUE(handlers[0]->nPings_.load() > 1);
  ASSERT_TRUE(handlers[1]->nPings_.load() > 1);
  ASSERT_TRUE(handlers[2]->nPings_.load() == 1);
  ASSERT_TRUE(handlers[3]->nPings_.load() == 1);
  ASSERT_TRUE(handlers[0]->nPings_.load() + handlers[1]->nPings_.load() == 102);

  // stop all servers
  for (auto& s : servers) {
    s->stop();
  }

  for (auto& t : thrs) {
    t->join();
  }
}

TEST(ThriftRouterTest, LocalGroupLongerThanConfigGroupTest) {
  FLAGS_always_prefer_local_host = true;
  updateConfigFile(g_config_v3);
  ThriftRouter<DummyServiceAsyncClient> router(
    "us-east-1c_some-more-info", g_config_path, common::parseConfig);

  std::vector<shared_ptr<DummyServiceAsyncClient>> v;
  shared_ptr<DummyServiceTestHandler> handlers[3];
  shared_ptr<ThriftServer> servers[3];
  unique_ptr<thread> thrs[3];

  tie(handlers[0], servers[0], thrs[0]) = makeServer(8090);
  tie(handlers[1], servers[1], thrs[1]) = makeServer(8091);
  tie(handlers[2], servers[2], thrs[2]) = makeServer(8092);
  sleep(1);

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 0), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 1), 3);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 2), 3);

  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ALL, 2, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 3);
  for (auto client : v) {
    EXPECT_NO_THROW(client->future_ping().get());
  }
  for (const auto& h : handlers) {
    EXPECT_EQ(h->nPings_.load(), 1);
  }

  for (int i = 0; i < 100; i ++) {
    std::vector<shared_ptr<DummyServiceAsyncClient>> v;
    EXPECT_EQ(
        router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, 2, &v),
        ReturnCode::OK);
    EXPECT_EQ(v.size(), 1);
    v[0]->future_ping().get();
  }

  ASSERT_TRUE(handlers[0]->nPings_.load() == 1);
  ASSERT_TRUE(handlers[1]->nPings_.load() == 101);
  ASSERT_TRUE(handlers[2]->nPings_.load() == 1);

  // stop all servers
  for (auto& s : servers) {
    s->stop();
  }

  for (auto& t : thrs) {
    t->join();
  }
}

TEST(ThriftRouterTest, UnreachableHost) {
  FLAGS_default_thrift_client_pool_threads = 1;
  FLAGS_client_connect_timeout_millis = 10;

  updateConfigFile(g_config_v5);
  ThriftRouter<DummyServiceAsyncClient> router(
    "", g_config_path, common::parseConfig);

  std::vector<shared_ptr<DummyServiceAsyncClient>> v;

  EXPECT_EQ(router.getShardNumberFor("user_pins"), 1);
  EXPECT_EQ(router.getHostNumberFor("user_pins", 0), 1);

  // We are able to create a client to an unreachable host. Because connect() is
  // async
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, 0, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 1);

  usleep(6000);

  // We will still be able to get a client after 6 ms. Because the connect
  // timeout is 10 ms
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, 0, &v),
    ReturnCode::OK);
  EXPECT_EQ(v.size(), 1);

  usleep(6000);

  // After timeout, we won't be able to get any client to the host. Because the
  // connection is bad and it's too soon to create a new connection.
  EXPECT_EQ(
    router.getClientsFor("user_pins", Role::ANY, Quantity::ONE, 0, &v),
    ReturnCode::BAD_HOST);
}

TEST(ThriftRouterTest, GetLeader) {
  // load the cluster info
  std::string content = g_config_v3;
  auto cluster = common::parseConfig(content, "");

  std::string segment = "user_pins";

  auto leader = cluster->getLeader(segment, 5);
  EXPECT_TRUE(leader.empty());

  leader = cluster->getLeader(segment, 0);
  EXPECT_EQ(leader.getHostStr(), "localhost");
  EXPECT_EQ(leader.getPort(), 8092);

  leader = cluster->getLeader(segment, 2);
  EXPECT_EQ(leader.getPort(), 8090);

}
int main(int argc, char** argv) {
  FLAGS_always_prefer_local_host = false;
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_channel_cleanup_min_interval_seconds = -1;
  return RUN_ALL_TESTS();
}
