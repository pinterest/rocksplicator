#pragma once
#include "rocksdb/db.h"
#include "rocksdb_replicator/thrift/gen-cpp2/Replicator.h"

namespace replicator {
class DbWrapper {
public:
  virtual rocksdb::Status WriteToLeader(const rocksdb::WriteOptions& options,
                                        rocksdb::WriteBatch* updates) = 0;
  virtual rocksdb::Status GetUpdatesFromLeader(
      rocksdb::SequenceNumber seq_number,
      std::unique_ptr<rocksdb::TransactionLogIterator>* iter) = 0;
  virtual uint64_t LatestSequenceNumber() = 0;
  virtual bool HandleReplicateResponse(Update* update) = 0;
};
}  // namespace replicator