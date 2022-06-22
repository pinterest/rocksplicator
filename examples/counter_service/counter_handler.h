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

#pragma once

#include <memory>
#include <string>

#include "examples/counter_service/counter_router.h"
#include "examples/counter_service/thrift/gen-cpp2/Counter.h"
#include "rocksdb_admin/admin_handler.h"

namespace counter {

class CounterHandler
    : public CounterSvIf
    , virtual public admin::AdminHandler {
 public:
  CounterHandler(
    std::unique_ptr<::admin::ApplicationDBManager> db_manager,
    admin::RocksDBOptionsGenerator rocksdb_options,
    std::unique_ptr<CounterRouter> router,
    rocksdb::WriteOptions write_options,
    rocksdb::ReadOptions read_options)
      : AdminHandler(std::move(db_manager), std::move(rocksdb_options))
      , router_(std::move(router))
      , write_options_(std::move(write_options))
      , read_options_(std::move(read_options)) {}

  virtual ~CounterHandler() {}

  void async_tm_getCounter(
      std::unique_ptr<apache::thrift::HandlerCallback<
        std::unique_ptr<::counter::GetResponse>>> callback,
      std::unique_ptr<::counter::GetRequest> request) override;

  void async_tm_setCounter(
      std::unique_ptr<apache::thrift::HandlerCallback<
        std::unique_ptr<::counter::SetResponse>>> callback,
      std::unique_ptr<::counter::SetRequest> request) override;

  void async_tm_bumpCounter(
      std::unique_ptr<apache::thrift::HandlerCallback<
        std::unique_ptr<::counter::BumpResponse>>> callback,
      std::unique_ptr<::counter::BumpRequest> request) override;

 private:
  std::shared_ptr<::admin::ApplicationDB> getDB(const std::string& db_name,
                                                CounterException* ex);
  std::unique_ptr<CounterRouter> router_;
  const rocksdb::WriteOptions write_options_;
  const rocksdb::ReadOptions read_options_;
};

}  // namespace counter
