#include "common/network_util.h"

#include "gtest/gtest.h"
#include "folly/SocketAddress.h"


namespace common {

TEST(NetworkUtilTest, GetNetworkAddressStr) {
  folly::SocketAddress addr;
  EXPECT_EQ(common::getNetworkAddressStr(addr), "uninitialized_addr");

  EXPECT_THROW(addr.setFromIpPort("bad-ip", 1234), std::runtime_error);
  EXPECT_EQ(common::getNetworkAddressStr(addr), "unknown_addr");

  addr.setFromIpPort("255.254.253.252", 8888);
  EXPECT_EQ(common::getNetworkAddressStr(addr), "255.254.253.252");
  addr.setFromIpPort("2620:0:1cfe:face:b00c::3:65535");
  EXPECT_EQ(common::getNetworkAddressStr(addr), "2620:0:1cfe:face:b00c::3");
}

}  // namespace common

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
