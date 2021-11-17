#include "common/dbconfig.h"

#include <string>
#include <glog/logging.h>
#include "gtest/gtest.h"


namespace common {
  /**
   *  use https://www.freeformatter.com/json-escape.html#ad-output
   *  to escape & unescape
   */

TEST(DBConfigTest, Basic) {
  auto cfgMgr = DBConfigManager::get();
  std::string content = R"({
   "dataset": {
    "testdataset" : {
        "ack_mode" : 1
      }
    } 
  })";

  EXPECT_FALSE(cfgMgr->hasValidData());
  cfgMgr->loadJsonStr(content);
  EXPECT_TRUE(cfgMgr->hasValidData());
  
  EXPECT_EQ(cfgMgr->getReplicationMode("dbtest00001"), 0);
  EXPECT_EQ(cfgMgr->getReplicationMode("testdataset00001"), 1);
  EXPECT_NE(cfgMgr->getReplicationMode("db1_00001"), 1);
  EXPECT_NE(cfgMgr->getReplicationMode("testdataset"), 1);

  EXPECT_EQ(cfgMgr->getReplicationMode("dbtest00001", 1), 1);
}

TEST(DBConfigTest, BasicFromObject) {
  auto cfgMgr = DBConfigManager::get();
  std::string content = R"({
   "dataset": {
    "testdataset" : {
        "ack_mode" : 1
      }
    }
  })";

  Json::Value root;
  Json::Reader reader;
  if (!reader.parse(content, root) || !root.isObject()) {
    LOG(ERROR) << "Could not parse json config :" << content;
  }

  cfgMgr->loadJsonObject(root);

  EXPECT_EQ(cfgMgr->getReplicationMode("dbtest00001"), 0);
  EXPECT_EQ(cfgMgr->getReplicationMode("testdataset00001"), 1);
  EXPECT_NE(cfgMgr->getReplicationMode("db1_00001"), 1);
  EXPECT_NE(cfgMgr->getReplicationMode("testdataset"), 1);

  EXPECT_EQ(cfgMgr->getReplicationMode("dbtest00001", 1), 1);
}

TEST(DBConfigTest, InvalidContent) {
  auto cfgMgr = DBConfigManager::get();
  std::string content = R"({
  })";

  Json::Value root;
  Json::Reader reader;
  if (!reader.parse(content, root) || !root.isObject()) {
    LOG(ERROR) << "Could not parse json config :" << content;
  }

  EXPECT_FALSE(cfgMgr->loadJsonObject(root));
}

TEST(DBConfigTest, EmptyContent) {
  auto cfgMgr = DBConfigManager::get();
  std::string content = R"()";

  Json::Value root;
  Json::Reader reader;
  if (!reader.parse(content, root) || !root.isObject()) {
    LOG(ERROR) << "Could not parse json config :" << content;
  }

  EXPECT_FALSE(cfgMgr->loadJsonObject(root));
}

}  // namespace common

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
