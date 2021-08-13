#include "rocksdb_admin/utils.h"

#include <boost/filesystem.hpp>
#include <string>
#include "common/file_util.h"
#include "gtest/gtest.h"
#include "rocksdb_admin/gen-cpp2/rocksdb_admin_types.h"

namespace filesystem = boost::filesystem;

namespace admin {

TEST(UtilsTest, ThriftSeder) {
  // decode Meta from empty string is !ok
  std::string content;
  DBMetaData meta;
  EXPECT_FALSE(DecodeThriftStruct(content, &meta));
  EXPECT_TRUE(meta.db_name.empty());
  EXPECT_FALSE(meta.__isset.s3_bucket);
  EXPECT_FALSE(meta.__isset.s3_path);
}

TEST(UtilsTest, ThriftSederWithFileUtils) {
  std::string base_dir = "/tmp";
  std::string filename = "thrift_seder";
  std::string expected_path = base_dir + "/" + filename;
  filesystem::remove(expected_path);

  DBMetaData meta;
  meta.set_db_name("test00000");
  meta.set_s3_bucket("hello-bucket");
  meta.set_s3_path("test/part-00000-");
  std::string encodedMeta;
  EncodeThriftStruct(meta, &encodedMeta);
  auto path =
      common::FileUtil::createFileWithContent(base_dir, filename, encodedMeta);

  std::string content;
  common::FileUtil::readFileToString(path, &content);
  EXPECT_EQ(content, encodedMeta);

  DBMetaData decodedMeta;
  DecodeThriftStruct(content, &decodedMeta);
  EXPECT_EQ(decodedMeta.db_name, "test00000");
  EXPECT_EQ(decodedMeta.s3_bucket, "hello-bucket");
  EXPECT_EQ(decodedMeta.s3_path, "test/part-00000-");
}

}  // namespace admin

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
