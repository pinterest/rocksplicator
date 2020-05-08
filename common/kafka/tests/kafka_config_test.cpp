//
// Test for read_conf_file
//

#include "common/kafka/kafka_config.h"
#include "gtest/gtest.h"

namespace kafka {

TEST(KafkaConfig, TestConfigFile) {
  std::shared_ptr<ConfigMap> configMap = std::shared_ptr<ConfigMap>(new ConfigMap);
  EXPECT_TRUE(read_conf_file("common/client_config.properties", configMap));
  EXPECT_TRUE(configMap->find("enable.sparse.connections") != configMap->end());
  EXPECT_TRUE(configMap->find("not_enabled_config_file") == configMap->end());
}
} // namespace kafka

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
