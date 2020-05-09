//
// Test for read_conf_file
//

#include <string>

#include <stdio.h>  /* defines FILENAME_MAX */
#include <unistd.h>
#define GetCurrentDir getcwd
#include<iostream>

#include "gtest/gtest.h"
#include "common/kafka/kafka_config.h"

namespace kafka {

std::string GetCurrentWorkingDir() {
  char buff[FILENAME_MAX];
  GetCurrentDir( buff, FILENAME_MAX );
  std::string current_working_dir(buff);
  return current_working_dir;
}

class KafkaConfigTest : public ::testing::Test {};

TEST_F(KafkaConfigTest, BasicTest) {
  EXPECT_TRUE(true);
}

TEST_F(KafkaConfigTest, SecondBasicTest) {
  EXPECT_TRUE(true);
}

TEST_F(KafkaConfigTest, ConfigFileTest) {

  std::cout << GetCurrentWorkingDir() << std::endl;
  std::cout.flush();

  std::shared_ptr<ConfigMap> configMap = std::shared_ptr<ConfigMap>(new ConfigMap);

  std::string configFile = std::string("client_config.properties");
  EXPECT_TRUE(KafkaConfig::read_conf_file(configFile, configMap, true));
  /*
  EXPECT_TRUE(configMap->find("enable.sparse.connections") != configMap->end());
  EXPECT_TRUE(configMap->find("enable.sparse.connections")->second.first == "true");
  EXPECT_TRUE(configMap->find("enable.sparse.connections")->second.second);
  EXPECT_TRUE(configMap->find("timeout_millis") != configMap->end());
  EXPECT_TRUE(configMap->find("timeout_millis")->second.first == "1200");
  EXPECT_TRUE(configMap->find("timeout_millis")->second.second);
  EXPECT_TRUE(configMap->find("socket_timeout_ms") != configMap->end());
  EXPECT_TRUE(configMap->find("socket_timeout_ms")->first == "socket_timeout_ms");
  EXPECT_TRUE(configMap->find("socket_timeout_ms")->second.first == "300");
  EXPECT_TRUE(configMap->find("socket_timeout_ms")->second.second);
  EXPECT_TRUE(configMap->find("not_enabled_config_file") == configMap->end());
   */
}

} // namespace kafka

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
