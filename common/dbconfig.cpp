// Copyright 2021-present Pinterest. All Rights Reserved.
//
// @author Premkumar (prem@pinterest.com)
//

#include "common/dbconfig.h"
#include "common/segment_utils.h"
#include <utility>

#include <gflags/gflags.h>
#include <glog/logging.h>

DECLARE_int32(replicator_replication_mode);

namespace common {

DBConfigManager* DBConfigManager::get() {
  static DBConfigManager instance;
  return &instance;
}

DBConfigManager::DBConfigManager() : dbConfig_(new DBConfig()), fDataLoaded_(false) {
}

bool DBConfigManager::loadJsonStr(const std::string& content) {
  Json::Value root;
  Json::Reader reader;
  if (!reader.parse(content, root) || !root.isObject()) {
    LOG(ERROR) << "Could not load config";
    return false;
  }

  return loadJsonObject(root);
}

bool DBConfigManager::loadJsonObject(const Json::Value& root) {
  try {
    auto newConfig = parseConfig(root);
    if (newConfig == nullptr) {
      return false;
    }

    LOG(INFO) << "Successfully updated config";
    {
      folly::SharedMutex::WriteHolder holder(lock);
      dbConfig_.reset();
      dbConfig_.swap(newConfig);
      fDataLoaded_.store(true);
    }
    return true;
  } catch (std::exception& ex) {
    LOG(ERROR) << "Exception from parseConfig: " << ex.what();
    return false;
  }
}

DBConfig::uptr DBConfigManager::parseConfig(const Json::Value& root) {
  static const std::string ACK_MODE = "ack_mode";
  static const std::string DATASET = "dataset";

  // invalid content
  if (!root.isObject()) {
    return nullptr;
  }
  std::unique_ptr<DBConfig> newDBConfig(new DBConfig());

  if (root.isMember(DATASET)) {
    // parse dataset specific config
    auto jsonDataSetConfigs = root[DATASET];

    for (const auto& dataset : jsonDataSetConfigs.getMemberNames()) {
      std::shared_ptr<Config> config = std::make_shared<Config>();
      const auto& jsonDataSetConfig = jsonDataSetConfigs[dataset];
      bool isSet = false; /* creating this for future multiple assigns */

      if(jsonDataSetConfig.isMember(ACK_MODE)) {
        config->replication_mode = jsonDataSetConfig[ACK_MODE].asUInt();
        isSet = true;
      }

      if (isSet) {
        newDBConfig->dataSetConfigMap[dataset] = config;
      }
    }
  }
  if (newDBConfig->dataSetConfigMap.size() == 0) {
    // if the json file is empty, then all settings go to 
    // default. 
  }
  return newDBConfig;
}

/* conf getter functions */
ConfigPtr DBConfigManager::getConfig(const std::string& dbName) const {
  folly::SharedMutex::ReadHolder holder(lock);
  auto dsName = DbNameToSegment(dbName);
  const auto& dsConfigMap = dbConfig_->dataSetConfigMap;
  const auto dsIter = dsConfigMap.find(dsName);
  if (dsIter == dsConfigMap.end()) {
    return nullptr;
  } else {
    return dsIter->second;
  }
}

uint DBConfigManager::getReplicationMode(const std::string& dbName, uint defValue) const {
  const ConfigPtr config = getConfig(dbName);
  if (config != nullptr) {
    return config->replication_mode;
  }

  return defValue;
}

}  // namespace common
