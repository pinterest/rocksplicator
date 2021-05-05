#include "common/file_util.h"

#include <boost/filesystem.hpp>
#include <string>
#include "gtest/gtest.h"

namespace filesystem = boost::filesystem;

namespace common {
TEST(FileUtilTest, Touch) {
  std::string path = "/tmp/_SUCCESS";
  filesystem::remove(path);
  EXPECT_FALSE(filesystem::exists(path));

  FileUtil::touch(path);
  EXPECT_TRUE(filesystem::exists(path));
}

TEST(FileUtilTest, CreateSuccessFile) {
  std::string path = "/tmp/_SUCCESS";
  filesystem::remove(path);
  EXPECT_FALSE(filesystem::exists(path));

  std::string succ_path = FileUtil::createSuccessFile("/tmp");
  EXPECT_TRUE(filesystem::exists(succ_path));
  EXPECT_EQ(succ_path, path);

  std::string nonexist_dir = "/tmp/no-exist";
  filesystem::remove(nonexist_dir);
  EXPECT_THROW(FileUtil::createSuccessFile(nonexist_dir), std::runtime_error);
}

TEST(FileUtilTest, CreateFileWithContent) {
  std::string base_dir = "/tmp";
  std::string filename = "CreateFileWithContent";
  std::string expected_path = base_dir + "/" + filename;
  filesystem::remove(expected_path);

  auto path = FileUtil::createFileWithContent(base_dir, filename, "hello");
  EXPECT_EQ(path, expected_path);

  std::string content;
  FileUtil::readFileToString(path, &content);
  EXPECT_EQ(content, "hello");
}

}  // namespace common

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
