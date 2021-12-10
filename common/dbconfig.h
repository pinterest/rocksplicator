// Copyright 2021-present Pinterest. All Rights Reserved.
//
// @author Premkumar (prem@pinterest.com)
//

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/config.h"
#include "common/jsoncpp/include/json/json.h"
#include <folly/SharedMutex.h>

/**
 * {
 *   "dataset" : {
 *      // dataset level configs
 *        "ad_query_join" : {
 *              "ack_mode" : 1,
 *       }
 *    }
 * }
 */

namespace common {

struct DBConfig {
  std::unordered_map<std::string, ConfigPtr> dataSetConfigMap;
  using uptr  = std::unique_ptr<const DBConfig>;
};



/**
 * DBConfigManager manages the per-dataset and service level
 * configuration for Rocksplicator.
 * 
 * The actual config is managed by the upper layer service and
 * is passed on via loadJsonObject or loadJsonStr
 * 
 * Thread safety is guaranteed by atomically loading and swapping 
 * the underlying ptr of the shared_ptr
 * 
 * Dangling ptrs of internals of the swapped object is avoided by using 
 * shared_ptrs all over.
 * 
 * This will be used via a static singleton instance
 */
class DBConfigManager {
public:
  static DBConfigManager* get();


  /**
   * Both the load functions are used to load data after instantiation
   * During init there may not be data for it to load.
   **/
  bool hasValidData() const {
    return fDataLoaded_.load();
  };
  /* load from a json object */
  bool loadJsonObject(const Json::Value& root);
  /* load from json string */
  bool loadJsonStr(const std::string& data);

  /* conf getter functions */
  uint getReplicationMode(const std::string& dbName, uint defValue = 0) const;

private:
  /* No direct instantiation. Use the global static via ::get()*/
  DBConfigManager();
  DBConfig::uptr dbConfig_;
  ConfigPtr getConfig(const std::string& dbName) const;
  DBConfig::uptr parseConfig(const Json::Value& root);
  std::atomic<bool> fDataLoaded_;
  mutable folly::SharedMutex lock;
};
}  // namespace common
