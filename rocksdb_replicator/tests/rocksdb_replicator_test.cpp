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

#include <string>
#include <vector>

#include "gtest/gtest.h"

// we need this hack to use RocksDBReplicator::RocksDBReplicator(), which is
// private
#define private public
#include "rocksdb_replicator/rocksdb_replicator.h"

using folly::SocketAddress;
using replicator::DBRole;
using replicator::ReturnCode;
using replicator::RocksDBReplicator;
using rocksdb::DB;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::Status;
using rocksdb::WriteBatch;
using rocksdb::WriteOptions;
using std::chrono::milliseconds;
using std::shared_ptr;
using std::string;
using std::this_thread::sleep_for;
using std::to_string;
using std::unique_ptr;
using std::vector;

DECLARE_int32(replicator_pull_delay_on_error_ms);
DECLARE_int32(rocksdb_replicator_port);

shared_ptr<DB> cleanAndOpenDB(const string& path) {
  EXPECT_EQ(system(("rm -rf " + path).c_str()), 0);
  DB* db;
  Options options;
  options.create_if_missing = true;
  EXPECT_TRUE(DB::Open(options, path, &db).ok());
  return shared_ptr<DB>(db);
}

TEST(RocksDBReplicatorTest, Basics) {
  auto replicator = RocksDBReplicator::instance();
  EXPECT_TRUE(replicator != nullptr);
  EXPECT_EQ(replicator->removeDB("non_exist_db"), ReturnCode::DB_NOT_FOUND);
  WriteOptions options;
  EXPECT_EQ(replicator->write("non_exist_db", options, nullptr),
            ReturnCode::DB_NOT_FOUND);

  auto db_master = cleanAndOpenDB("/tmp/db_master");
  auto db_slave = cleanAndOpenDB("/tmp/db_slave");

  RocksDBReplicator::ReplicatedDB* replicated_db_master;
  RocksDBReplicator::ReplicatedDB* replicated_db_slave;
  SocketAddress addr("127.0.0.1", FLAGS_rocksdb_replicator_port);

  EXPECT_EQ(replicator->addDB("master", db_master, DBRole::MASTER,
                              folly::SocketAddress(),
                              &replicated_db_master), ReturnCode::OK);
  EXPECT_EQ(replicator->addDB("master", db_master, DBRole::MASTER,
                              folly::SocketAddress(),
                              &replicated_db_master), ReturnCode::DB_PRE_EXIST);
  EXPECT_EQ(replicator->addDB("slave", db_slave, DBRole::SLAVE,
                              addr, &replicated_db_slave), ReturnCode::OK);

  Status status;
  WriteBatch updates;
  updates.Put("key", "value");
  EXPECT_EQ(replicator->write("slave", options, &updates),
            ReturnCode::WRITE_TO_SLAVE);
  EXPECT_THROW(replicated_db_slave->Write(options, &updates), ReturnCode);
  EXPECT_EQ(replicator->write("master", options, &updates),
            ReturnCode::OK);
  EXPECT_NO_THROW(status = replicated_db_master->Write(options, &updates));
  EXPECT_TRUE(status.ok());

  EXPECT_EQ(replicator->removeDB("slave"), ReturnCode::OK);
  EXPECT_EQ(replicator->removeDB("master"), ReturnCode::OK);
  EXPECT_EQ(replicator->removeDB("master"), ReturnCode::DB_NOT_FOUND);
  EXPECT_EQ(replicator->write("slave", options, &updates),
            ReturnCode::DB_NOT_FOUND);
  EXPECT_EQ(replicator->write("master", options, &updates),
            ReturnCode::DB_NOT_FOUND);

  const char* expected_master_state =
"ReplicatedDB:\n\
  name: master\n\
  DBRole: LEADER\n\
  upstream_addr: unknown_addr\n\
  cur_seq_no: 2\n";
  const char* expected_slave_state =
"ReplicatedDB:\n\
  name: slave\n\
  DBRole: FOLLOWER\n\
  upstream_addr: 127.0.0.1\n\
  cur_seq_no: 0\n";
  EXPECT_EQ(replicated_db_master->Introspect(), std::string(expected_master_state));
  EXPECT_EQ(replicated_db_slave->Introspect(), std::string(expected_slave_state));
}

struct Host {
  explicit Host(int16_t port) {
    FLAGS_rocksdb_replicator_port = port;
    replicator_.reset(new RocksDBReplicator);
  }

  unique_ptr<RocksDBReplicator> replicator_;
};

TEST(RocksDBReplicatorTest, 1_master_1_slave) {
  int16_t master_port = 9092;
  int16_t slave_port = 9093;
  Host master(master_port);
  Host slave(slave_port);

  auto db_master = cleanAndOpenDB("/tmp/db_master");
  auto db_slave = cleanAndOpenDB("/tmp/db_slave");

  EXPECT_EQ(master.replicator_->addDB("shard1", db_master, DBRole::MASTER),
            ReturnCode::OK);
  SocketAddress addr_master("127.0.0.1", master_port);
  EXPECT_EQ(slave.replicator_->addDB("shard1", db_slave, DBRole::SLAVE,
                                     addr_master),
            ReturnCode::OK);

  EXPECT_EQ(db_master->GetLatestSequenceNumber(), 0);
  EXPECT_EQ(db_slave->GetLatestSequenceNumber(), 0);

  WriteOptions options;
  uint32_t n_keys = 100;
  for (uint32_t i = 0; i < n_keys; ++i) {
    WriteBatch updates;
    auto str = to_string(i);
    updates.Put(str + "key", str + "value");
    updates.Put(str + "key2", str + "value2");
    EXPECT_EQ(master.replicator_->write("shard1", options, &updates),
              ReturnCode::OK);
    EXPECT_EQ(db_master->GetLatestSequenceNumber(), i * 2 + 2);
  }

  while (db_slave->GetLatestSequenceNumber() < n_keys * 2) {
    sleep_for(milliseconds(100));
  }

  EXPECT_EQ(db_slave->GetLatestSequenceNumber(), n_keys * 2);
  ReadOptions read_options;
  for (uint32_t i = 0; i < n_keys; ++i) {
    auto str = to_string(i);
    string value;
    auto status = db_slave->Get(read_options, str + "key", &value);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(value, str + "value");

    status = db_slave->Get(read_options, str + "key2", &value);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(value, str + "value2");
  }
  EXPECT_EQ(db_slave->GetLatestSequenceNumber(), n_keys * 2);

  // remove the master db from the replication library, write more keys to the
  // master db. the slave doesn't have the new keys
  EXPECT_EQ(master.replicator_->removeDB("shard1"), ReturnCode::OK);
  for (uint32_t i = 0; i < n_keys; ++i) {
    WriteBatch updates;
    auto str = to_string(i);
    updates.Put(str + "new_key", str + "new_value");
    auto status = db_master->Write(options, &updates);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(db_master->GetLatestSequenceNumber(), i + 1 + n_keys * 2);
  }
  EXPECT_EQ(db_slave->GetLatestSequenceNumber(), n_keys * 2);
}

TEST(RocksDBReplicatorTest, 1_master_2_slaves_tree) {
  int16_t master_port = 9094;
  int16_t slave_port_1 = 9095;
  int16_t slave_port_2 = 9096;
  Host master(master_port);
  Host slave_1(slave_port_1);
  Host slave_2(slave_port_2);

  auto db_master = cleanAndOpenDB("/tmp/db_master");
  auto db_slave_1 = cleanAndOpenDB("/tmp/db_slave_1");
  auto db_slave_2 = cleanAndOpenDB("/tmp/db_slave_2");

  EXPECT_EQ(master.replicator_->addDB("shard1", db_master, DBRole::MASTER),
            ReturnCode::OK);
  SocketAddress addr_master("127.0.0.1", master_port);
  EXPECT_EQ(slave_1.replicator_->addDB("shard1", db_slave_1, DBRole::SLAVE,
                                       addr_master),
            ReturnCode::OK);
  EXPECT_EQ(slave_2.replicator_->addDB("shard1", db_slave_2, DBRole::SLAVE,
                                       addr_master),
            ReturnCode::OK);

  EXPECT_EQ(db_master->GetLatestSequenceNumber(), 0);
  EXPECT_EQ(db_slave_1->GetLatestSequenceNumber(), 0);
  EXPECT_EQ(db_slave_2->GetLatestSequenceNumber(), 0);

  WriteOptions options;
  uint32_t n_keys = 100;
  for (uint32_t i = 0; i < n_keys; ++i) {
    WriteBatch updates;
    auto str = to_string(i);
    updates.Put(str + "key", str + "value");
    EXPECT_EQ(master.replicator_->write("shard1", options, &updates),
              ReturnCode::OK);
    EXPECT_EQ(db_master->GetLatestSequenceNumber(), i + 1);
  }

  while (db_slave_1->GetLatestSequenceNumber() < n_keys ||
         db_slave_2->GetLatestSequenceNumber() < n_keys) {
    sleep_for(milliseconds(100));
  }

  EXPECT_EQ(db_slave_1->GetLatestSequenceNumber(), n_keys);
  EXPECT_EQ(db_slave_2->GetLatestSequenceNumber(), n_keys);
  ReadOptions read_options;
  for (uint32_t i = 0; i < n_keys; ++i) {
    auto str = to_string(i);
    string value;
    auto status = db_slave_1->Get(read_options, str + "key", &value);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(value, str + "value");

    status = db_slave_2->Get(read_options, str + "key", &value);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(value, str + "value");
  }
  EXPECT_EQ(db_slave_1->GetLatestSequenceNumber(), n_keys);
  EXPECT_EQ(db_slave_2->GetLatestSequenceNumber(), n_keys);
}

TEST(RocksDBReplicatorTest, 1_master_2_slaves_chain) {
  int16_t master_port = 9097;
  int16_t slave_port_1 = 9098;
  int16_t slave_port_2 = 9099;
  Host master(master_port);
  Host slave_1(slave_port_1);
  Host slave_2(slave_port_2);

  auto db_master = cleanAndOpenDB("/tmp/db_master");
  auto db_slave_1 = cleanAndOpenDB("/tmp/db_slave_1");
  auto db_slave_2 = cleanAndOpenDB("/tmp/db_slave_2");

  EXPECT_EQ(master.replicator_->addDB("shard1", db_master, DBRole::MASTER),
            ReturnCode::OK);
  SocketAddress addr_master("127.0.0.1", master_port);
  EXPECT_EQ(slave_1.replicator_->addDB("shard1", db_slave_1, DBRole::SLAVE,
                                       addr_master),
            ReturnCode::OK);
  SocketAddress addr_slave_1("127.0.0.1", slave_port_1);
  EXPECT_EQ(slave_2.replicator_->addDB("shard1", db_slave_2, DBRole::SLAVE,
                                       addr_slave_1),
            ReturnCode::OK);

  EXPECT_EQ(db_master->GetLatestSequenceNumber(), 0);
  EXPECT_EQ(db_slave_1->GetLatestSequenceNumber(), 0);
  EXPECT_EQ(db_slave_2->GetLatestSequenceNumber(), 0);

  WriteOptions options;
  uint32_t n_keys = 100;
  for (uint32_t i = 0; i < n_keys; ++i) {
    WriteBatch updates;
    auto str = to_string(i);
    updates.Put(str + "key", str + "value");
    EXPECT_EQ(master.replicator_->write("shard1", options, &updates),
              ReturnCode::OK);
    EXPECT_EQ(db_master->GetLatestSequenceNumber(), i + 1);
  }

  while (db_slave_2->GetLatestSequenceNumber() < n_keys) {
    sleep_for(milliseconds(100));
  }

  EXPECT_EQ(db_slave_1->GetLatestSequenceNumber(), n_keys);
  EXPECT_EQ(db_slave_2->GetLatestSequenceNumber(), n_keys);
  ReadOptions read_options;
  for (uint32_t i = 0; i < n_keys; ++i) {
    auto str = to_string(i);
    string value;
    auto status = db_slave_1->Get(read_options, str + "key", &value);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(value, str + "value");

    status = db_slave_2->Get(read_options, str + "key", &value);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(value, str + "value");
  }
  EXPECT_EQ(db_slave_1->GetLatestSequenceNumber(), n_keys);
  EXPECT_EQ(db_slave_2->GetLatestSequenceNumber(), n_keys);

  // remove the middle node, and write some more keys to the master
  EXPECT_EQ(slave_1.replicator_->removeDB("shard1"), ReturnCode::OK);
  for (uint32_t i = 0; i < n_keys; ++i) {
    WriteBatch updates;
    auto str = to_string(i);
    updates.Put(str + "new_key", str + "new_value");
    EXPECT_EQ(master.replicator_->write("shard1", options, &updates),
              ReturnCode::OK);
    EXPECT_EQ(db_master->GetLatestSequenceNumber(), i + n_keys + 1);
  }

  // non of slaves got them
  EXPECT_EQ(db_slave_1->GetLatestSequenceNumber(), n_keys);
  EXPECT_EQ(db_slave_2->GetLatestSequenceNumber(), n_keys);

  // add the middle node back
  EXPECT_EQ(slave_1.replicator_->addDB("shard1", db_slave_1, DBRole::SLAVE,
                                       addr_master),
            ReturnCode::OK);

  while (db_slave_2->GetLatestSequenceNumber() < 2 * n_keys) {
    sleep_for(milliseconds(100));
  }

  EXPECT_EQ(db_slave_1->GetLatestSequenceNumber(), 2 * n_keys);
  EXPECT_EQ(db_slave_2->GetLatestSequenceNumber(), 2 * n_keys);
  for (uint32_t i = 0; i < n_keys; ++i) {
    auto str = to_string(i);
    string value;
    auto status = db_slave_1->Get(read_options, str + "new_key", &value);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(value, str + "new_value");

    status = db_slave_2->Get(read_options, str + "new_key", &value);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(value, str + "new_value");
  }
  EXPECT_EQ(db_slave_1->GetLatestSequenceNumber(), 2 * n_keys);
  EXPECT_EQ(db_slave_2->GetLatestSequenceNumber(), 2 * n_keys);
}

TEST(RocksDBReplicatorTest, Stress) {
  int16_t port_1 = 8081;
  int16_t port_2 = 8082;
  int16_t port_3 = 8083;
  Host host_1(port_1);
  Host host_2(port_2);
  Host host_3(port_3);
  int n_shards = 20;
  uint32_t n_keys = 100;

  vector<shared_ptr<DB>> db_masters;
  vector<shared_ptr<DB>> db_slaves_1;
  vector<shared_ptr<DB>> db_slaves_2;

  for (int i = 0; i < n_shards; ++i) {
    auto str = to_string(i);
    db_masters.emplace_back(cleanAndOpenDB("/tmp/db_master" + str));
    db_slaves_1.emplace_back(cleanAndOpenDB("/tmp/db_slave_1" + str));
    db_slaves_2.emplace_back(cleanAndOpenDB("/tmp/db_slave_2" + str));
  }

  vector<Host*> hosts { &host_1, &host_2, &host_3 };
  vector<SocketAddress> addresses;
  addresses.emplace_back("127.0.0.1", port_1);
  addresses.emplace_back("127.0.0.1", port_2);
  addresses.emplace_back("127.0.0.1", port_3);
  for (int i = 0; i < n_shards; ++i) {
    auto shard = "shard" + to_string(i);
    int start = i % hosts.size();

    EXPECT_EQ(hosts[start]->replicator_->addDB(shard, db_masters[i],
                                              DBRole::MASTER),
              ReturnCode::OK);
    EXPECT_EQ(hosts[(start + 1) % hosts.size()]->replicator_->addDB(
        shard, db_slaves_1[i], DBRole::SLAVE, addresses[start]),
              ReturnCode::OK);
    EXPECT_EQ(hosts[(start + 2) % hosts.size()]->replicator_->addDB(
        shard, db_slaves_2[i], DBRole::SLAVE, addresses[start]),
              ReturnCode::OK);
  }

  WriteOptions options;
  for (uint32_t i = 0; i < n_keys; ++i) {
    for (int j = 0; j < n_shards; ++j) {
      auto str = to_string(i);
      auto shard = "shard" + to_string(j);
      WriteBatch updates;
      updates.Put(str + "key", str + "value");

      auto code = host_1.replicator_->write(shard, options, &updates);
      EXPECT_TRUE(code == ReturnCode::OK || code == ReturnCode::WRITE_TO_SLAVE);
      code = host_2.replicator_->write(shard, options, &updates);
      EXPECT_TRUE(code == ReturnCode::OK || code == ReturnCode::WRITE_TO_SLAVE);
      code = host_3.replicator_->write(shard, options, &updates);
      EXPECT_TRUE(code == ReturnCode::OK || code == ReturnCode::WRITE_TO_SLAVE);
    }
  }

  ReadOptions read_options;
  for (int i = 0; i < n_shards; ++i) {
    EXPECT_EQ(db_masters[i]->GetLatestSequenceNumber(), n_keys);
    while (db_slaves_1[i]->GetLatestSequenceNumber() < n_keys ||
           db_slaves_2[i]->GetLatestSequenceNumber() < n_keys) {
      sleep_for(milliseconds(100));
    }
    EXPECT_EQ(db_slaves_1[i]->GetLatestSequenceNumber(), n_keys);
    EXPECT_EQ(db_slaves_2[i]->GetLatestSequenceNumber(), n_keys);

    for (uint32_t j = 0; j < n_keys; ++j) {
      string value;
      auto str = to_string(j);
      auto status = db_masters[i]->Get(read_options, str + "key", &value);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(value, str + "value");

      status = db_slaves_1[i]->Get(read_options, str + "key", &value);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(value, str + "value");
      status = db_slaves_2[i]->Get(read_options, str + "key", &value);
      EXPECT_TRUE(status.ok());
      EXPECT_EQ(value, str + "value");
    }
  }
}

int main(int argc, char** argv) {
  FLAGS_replicator_pull_delay_on_error_ms = 100;
  ::testing::InitGoogleTest(&argc, argv);
  auto ret = RUN_ALL_TESTS();
  sleep_for(milliseconds(1000));
  return ret;
}

