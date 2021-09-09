#pragma once

#include "rocksdb_replicator/db_wrapper.h"
#include "rocksdb_replicator/replicator_stats.h"
#include "rocksdb_replicator/rocksdb_replicator.h"
#include "rocksdb_replicator/thrift/gen-cpp2/Replicator.h"

namespace replicator {
class TestDBProxy : public replicator::DbWrapper, public std::enable_shared_from_this<TestDBProxy> {
public:
  uint64_t LatestSequenceNumber() override;
  rocksdb::Status WriteToLeader(const rocksdb::WriteOptions& options,
                                rocksdb::WriteBatch* updates) override;
  rocksdb::Status GetUpdatesFromLeader(
      rocksdb::SequenceNumber seq_number,
      std::unique_ptr<rocksdb::TransactionLogIterator>* iter) override;
  bool HandleReplicateResponse(Update* update) override;
  TestDBProxy(const std::string& db_name, rocksdb::SequenceNumber seq_no = 0);

private:
  rocksdb::SequenceNumber seq_no_;
  const std::string db_name_;
};

}  // namespace replicator