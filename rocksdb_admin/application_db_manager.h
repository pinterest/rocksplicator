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


#pragma once

#include <map>
#include <shared_mutex>
#include <string>

#include "rocksdb/db.h"
#include "rocksdb_admin/application_db.h"

namespace admin {

// This class manages application rocksdb instances, it offers functionality to
// add/remove/get application rocksdb instance.
// Note: this class is thread-safe.
class ApplicationDBManager {
 public:
  ApplicationDBManager();

  // Add a rocksdb instance into db manager.
  // db_name:        (IN) The name of the associated rocksdb instance
  // db:             (IN) The unique pointer of the associated rocksdb instance
  // role:           (IN) Replicating role of the associated rocksdb instance
  // error_message: (OUT) This field will be set if something goes wrong
  //
  // Return true on success
  bool addDB(const std::string& db_name,
             std::unique_ptr<rocksdb::DB> db,
             replicator::DBRole role,
             std::string* error_message);

  // Add a rocksdb instance into db manager.
  // db_name:        (IN) Name of the associated rocksdb instance
  // db:             (IN) The unique pointer of the associated rocksdb instance
  // role:           (IN) Replicating role of the associated rocksdb instance
  // upstream_addr   (IN) Address of upstream rocksdb instance
  // error_message: (OUT) This field will be set if something goes wrong
  //
  // Return true on success
  bool addDB(const std::string& db_name,
             std::unique_ptr<rocksdb::DB> db,
             replicator::DBRole role,
             std::unique_ptr<folly::SocketAddress> upstream_addr,
             std::string* error_message);

  // Get ApplicationDB instance of the given name
  // Returned db is not supposed to be long held by client
  // db_name:        (IN) Name of the ApplicationDB instance to be returned
  // error_message: (OUT) This field will be set if something goes wrong
  //
  // Return non-null pointer on success
  const std::shared_ptr<ApplicationDB> getDB(const std::string& db_name,
                                             std::string* error_message);

  // Remove ApplicationDB instance of the given name
  // db_name:        (IN) Name of the ApplicationDB instance to be removed
  // error_message: (OUT) This field will be set if something goes wrong
  //
  // Return non-null pointer on success
  std::unique_ptr<rocksdb::DB> removeDB(const std::string& db_name,
                                        std::string* error_message);

  // Dump stats for all DBs as a text string
  std::string DumpDBStatsAsText() const;

  ~ApplicationDBManager();

 private:
  std::unordered_map<std::string, std::shared_ptr<ApplicationDB>> dbs_;
  mutable std::shared_mutex dbs_lock_;

  void waitOnApplicationDBRef(const std::shared_ptr<ApplicationDB>& db);
};

}  // namespace admin
