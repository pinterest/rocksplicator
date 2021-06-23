#include "rocksdb_replicator/rocksdb_wrapper.h"

namespace replicator {
uint64_t RocksDbWrapper::LatestSequenceNumber() { return db_->GetLatestSequenceNumber(); }
rocksdb::Status RocksDbWrapper::WriteToLeader(const rocksdb::WriteOptions& options,
                                              rocksdb::WriteBatch* updates) {
  return db_->Write(options, updates);
}
rocksdb::Status RocksDbWrapper::GetUpdatesFromLeader(
    rocksdb::SequenceNumber seq_number, std::unique_ptr<rocksdb::TransactionLogIterator>* iter) {
  return db_->GetUpdatesSince(seq_number, iter);
}
bool RocksDbWrapper::HandleReplicateResponse(Update update) {
  // Is this bad for perf?
  // If so pass in the IOBuf returned by coalesce separately
  auto byteRange = update.raw_data.coalesce();
  rocksdb::WriteBatch write_batch(
      std::string(reinterpret_cast<const char*>(byteRange.data()), byteRange.size()));
  write_batch.PutLogData(
      rocksdb::Slice(reinterpret_cast<const char*>(&update.timestamp), sizeof(update.timestamp)));

  auto status = db_->Write(write_options_, &write_batch);
  bool ret_status = status.ok();
  if (!ret_status) {
    LOG(ERROR) << "Failed to apply updates to FOLLOWER " << db_name_ << " " << status.ToString();
  }
  return ret_status;
}

RocksDbWrapper::RocksDbWrapper(const std::string& db_name, std::shared_ptr<rocksdb::DB> db)
    : db_name_(db_name), db_(std::move(db)), write_options_() {}
}  // namespace replicator