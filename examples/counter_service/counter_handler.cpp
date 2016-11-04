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


#include "examples/counter_service/counter_handler.h"

#include <string>

#include "examples/counter_service/stats_enum.h"
#include "common/stats/stats.h"
#include "common/timer.h"


namespace counter {

std::shared_ptr<::admin::ApplicationDB> CounterHandler::getDB(
    const std::string& db_name,
    CounterException* ex) {
  ::admin::AdminException e;
  auto db = ::admin::AdminHandler::getDB(db_name, &e);
  if (ex && db == nullptr) {
    ex->code = ErrorCode::DB_NOT_FOUND;
    ex->msg = std::move(e.message);
  }

  return db;
}

void CounterHandler::async_tm_getCounter(
    std::unique_ptr<apache::thrift::HandlerCallback<
      std::unique_ptr<::counter::GetResponse>>> callback,
    std::unique_ptr<::counter::GetRequest> request) {
  common::Stats::get()->Incr(kApiGetCounter);
  common::Timer timer(kApiGetCounterMs);

  CounterException ex;
  if (request->need_routing) {
    request->need_routing = false;
    std::vector<std::shared_ptr<CounterAsyncClient>> clients;
    router_->GetClientsFor(request->segment,
                           request->counter_name,
                           true /* for_read */,
                           &clients);
    if (clients.empty()) {
      ex.code = ErrorCode::SERVER_NOT_FOUND;
      ex.msg = "Server not found for getting: " + request->counter_name;
      callback.release()->exceptionInThread(std::move(ex));
      return;
    }

    clients[0]->future_getCounter(*request).then(
      [ callback = std::move(callback) ]
      (folly::Try<::counter::GetResponse>&& t) mutable {
        if (t.hasException()) {
          callback.release()->exceptionInThread(t.exception());
        } else {
          callback.release()->resultInThread(std::move(t.value()));
        }
      });

    return;
  }

  auto db_name = router_->GetDBName(request->segment, request->counter_name);
  auto db = getDB(db_name, &ex);

  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(ex));
    return;
  }

  std::string value;
  auto status = db->Get(read_options_, request->counter_name, &value);
  if (!status.ok()) {
    ex.code = ErrorCode::ROCKSDB_ERROR;
    ex.msg = status.ToString();
    callback.release()->exceptionInThread(std::move(ex));
    return;
  }

  if (value.size() != sizeof(int64_t)) {
    ex.code = ErrorCode::CORRUPTED_DATA;
    ex.msg = "Corrupted data found";
    callback.release()->exceptionInThread(std::move(ex));
    return;
  }


  GetResponse res;
  memcpy(&res.counter_value, value.c_str(), value.size());
  callback.release()->resultInThread(res);
}

void CounterHandler::async_tm_setCounter(
    std::unique_ptr<apache::thrift::HandlerCallback<
      std::unique_ptr<::counter::SetResponse>>> callback,
    std::unique_ptr<::counter::SetRequest> request) {
  common::Stats::get()->Incr(kApiSetCounter);
  common::Timer timer(kApiSetCounterMs);

  CounterException ex;
  if (request->need_routing) {
    request->need_routing = false;
    std::vector<std::shared_ptr<CounterAsyncClient>> clients;
    router_->GetClientsFor(request->segment,
                           request->counter_name,
                           false /* for_read */,
                           &clients);
    if (clients.empty()) {
      ex.code = ErrorCode::SERVER_NOT_FOUND;
      ex.msg = "Server not found for setting: " + request->counter_name;
      callback.release()->exceptionInThread(std::move(ex));
      return;
    }

    clients[0]->future_setCounter(*request).then(
      [ callback = std::move(callback) ]
      (folly::Try<::counter::SetResponse>&& t) mutable {
        if (t.hasException()) {
          callback.release()->exceptionInThread(t.exception());
        } else {
          callback.release()->resultInThread(std::move(t.value()));
        }
      });

    return;
  }

  auto db_name = router_->GetDBName(request->segment, request->counter_name);
  auto db = getDB(db_name, &ex);

  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(ex));
    return;
  }

  rocksdb::WriteBatch write_batch;
  write_batch.Put(
    request->counter_name,
    rocksdb::Slice(reinterpret_cast<const char*>(&request->counter_value),
                   sizeof(request->counter_value)));

  auto status = db->Write(write_options_, &write_batch);
  if (status.ok()) {
    callback.release()->resultInThread(SetResponse());
    return;
  }

  ex.code = ErrorCode::ROCKSDB_ERROR;
  ex.msg = status.ToString();
  callback.release()->exceptionInThread(std::move(ex));
}

void CounterHandler::async_tm_bumpCounter(
    std::unique_ptr<apache::thrift::HandlerCallback<
      std::unique_ptr<::counter::BumpResponse>>> callback,
    std::unique_ptr<::counter::BumpRequest> request) {
  common::Stats::get()->Incr(kApiBumpCounter);
  common::Timer timer(kApiBumpCounterMs);

  CounterException ex;
  if (request->need_routing) {
    request->need_routing = false;
    std::vector<std::shared_ptr<CounterAsyncClient>> clients;
    router_->GetClientsFor(request->segment,
                           request->counter_name,
                           false /* for_read */,
                           &clients);
    if (clients.empty()) {
      ex.code = ErrorCode::SERVER_NOT_FOUND;
      ex.msg = "Server not found for bumping: " + request->counter_name;
      callback.release()->exceptionInThread(std::move(ex));
      return;
    }

    clients[0]->future_bumpCounter(*request).then(
      [ callback = std::move(callback) ]
      (folly::Try<::counter::BumpResponse>&& t) mutable {
        if (t.hasException()) {
          callback.release()->exceptionInThread(t.exception());
        } else {
          callback.release()->resultInThread(std::move(t.value()));
        }
      });

    return;
  }

  auto db_name = router_->GetDBName(request->segment, request->counter_name);
  auto db = getDB(db_name, &ex);

  if (db == nullptr) {
    callback.release()->exceptionInThread(std::move(ex));
    return;
  }

  rocksdb::WriteBatch write_batch;
  write_batch.Merge(
    request->counter_name,
    rocksdb::Slice(reinterpret_cast<const char*>(&request->counter_delta),
                   sizeof(request->counter_delta)));

  auto status = db->Write(write_options_, &write_batch);
  if (status.ok()) {
    callback.release()->resultInThread(BumpResponse());
    return;
  }

  ex.code = ErrorCode::ROCKSDB_ERROR;
  ex.msg = status.ToString();
  callback.release()->exceptionInThread(std::move(ex));
}

}  // namespace counter
