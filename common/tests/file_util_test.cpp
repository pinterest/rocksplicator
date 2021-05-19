#include "common/file_util.h"

#include <boost/filesystem.hpp>
#include <string>
#include "gtest/gtest.h"

namespace filesystem = boost::filesystem;

namespace common {

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
