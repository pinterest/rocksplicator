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

static const char* g_config_v1Group =
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
  "  \"127.0.0.1:8091:\": [\"00000:S\", \"00001:M\", \"00002:M\"],"
  "  \"127.0.0.1:8092:zone_c\": [\"00001:S\", \"00002:M\"]"
  "   },"
  "  \"interest_pins\": {"
  "  \"num_leaf_segments\": 2,"
  "  \"127.0.0.1:8090:zone_a\": [\"00000:M\"],"
  "  \"127.0.0.1:8091:\": [\"00001:S\"]"
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
  FLAGS_port = 8099; // a foreign host
  updateConfigFile("");
  ThriftRouter<DummyServiceAsyncClient> router(
          "127.0.0.1", g_config_path, common::parseConfig);
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
    "127.0.0.1", g_config_path, common::parseConfig);
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

TEST(ThriftRouterTest, LocalGroupTest) {
  FLAGS_port = 8090;
  updateConfigFile(g_config_v1Group);
  ThriftRouter<DummyServiceAsyncClient> router(
    "127.0.0.1", g_config_path, common::parseConfig);

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

  // Get the client from local Group
  // All requests should hit the local Group handler
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


TEST(ThriftRouterTest, ForeignGroupTest) {
  FLAGS_port = 8096;
  updateConfigFile(g_config_v1Group);
  ThriftRouter<DummyServiceAsyncClient> router(
    "127.0.0.1", g_config_path, common::parseConfig);

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

TEST(ThriftRouterTest, ForeignGroupMultiClientsTest) {
  FLAGS_port = 8099;
  updateConfigFile(g_config_v1Group);
  ThriftRouter<DummyServiceAsyncClient> router(
    "127.0.0.1", g_config_path, common::parseConfig);

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
  FLAGS_port = 8091;
  updateConfigFile(g_config_v3);
  ThriftRouter<DummyServiceAsyncClient> router(
    "127.0.0.1", g_config_path, common::parseConfig);


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

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  FLAGS_channel_cleanup_min_interval_seconds = -1;
  return RUN_ALL_TESTS();
}
