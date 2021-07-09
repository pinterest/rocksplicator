#include "rocksdb_replicator/test_db_proxy.h"

namespace replicator {
uint64_t TestDBProxy::LatestSequenceNumber() { return seq_no_; }
rocksdb::Status WriteToLeader(const rocksdb::WriteOptions& options, rocksdb::WriteBatch* updates) {
  throw ReturnCode::WRITE_TO_SLAVE;
}
rocksdb::Status TestDBProxy::GetUpdatesFromLeader(
    rocksdb::SequenceNumber seq_number, std::unique_ptr<rocksdb::TransactionLogIterator>* iter) {
  throw ReturnCode::WRITE_TO_SLAVE;
}
bool TestDBProxy::HandleReplicateResponse(Update* update) {
  // Is this bad for perf?
  // If so pass in the IOBuf returned by coalesce separately
  auto byteRange = update->raw_data.coalesce();
  LOG(INFO) << "Update at " << update->timestamp << ": "
            << folly::hexlify(std::string(reinterpret_cast<const char*>(byteRange.data())));
  seq_no_++;
  return true;
}

TestDBProxy::TestDBProxy(const std::string& db_name, rocksdb::SequenceNumber seq_no)
    : db_name_(db_name), seq_no_(seq_no) {}
}  // namespace replicator