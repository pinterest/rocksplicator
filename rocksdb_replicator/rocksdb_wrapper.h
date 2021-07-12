
#include "rocksdb_replicator/db_wrapper.h"
#include "rocksdb_replicator/replicator_stats.h"
#include "rocksdb_replicator/rocksdb_replicator.h"
#include "rocksdb_replicator/thrift/gen-cpp2/Replicator.h"

namespace replicator {
class RocksDbWrapper : public replicator::DbWrapper,
                       public std::enable_shared_from_this<RocksDbWrapper> {
public:
  uint64_t LatestSequenceNumber() override;
  rocksdb::Status WriteToLeader(const rocksdb::WriteOptions& options,
                                rocksdb::WriteBatch* updates) override;
  rocksdb::Status GetUpdatesFromLeader(
      rocksdb::SequenceNumber seq_number,
      std::unique_ptr<rocksdb::TransactionLogIterator>* iter) override;
  bool HandleReplicateResponse(Update* update) override;
  RocksDbWrapper(const std::string& db_name, std::shared_ptr<rocksdb::DB> db);

private:
  std::shared_ptr<rocksdb::DB> db_;
  const std::string db_name_;
  rocksdb::WriteOptions write_options_;
};

}  // namespace replicator