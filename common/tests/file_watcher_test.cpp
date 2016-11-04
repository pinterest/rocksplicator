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

//
// @author bol (bol@pinterest.com)
//

#include <folly/MPMCQueue.h>
#include <fstream>
#include <string>

#include "common/file_watcher.h"
#include "gtest/gtest.h"

using common::FileWatcher;
using folly::MPMCQueue;
using std::move;
using std::ofstream;
using std::string;

// DECLARE_int32(recheck_removed_file_interval_ms);

int RemoveFile(const string& file_name) {
  auto cmd = "rm " + file_name;
  return system(cmd.c_str());
}

void OverwriteFile(const string& file_name, const string& content) {
  ofstream os(file_name);
  EXPECT_TRUE(os);
  os << content;
}

void OverwriteAndTest(const string& file_name, const string& content,
                      const string* expected_content,
                      MPMCQueue<string>* queue) {
  OverwriteFile(file_name, content);
  string read_content;
  if (expected_content) {
    queue->blockingRead(read_content);
    EXPECT_EQ(*expected_content, read_content);
  }

  sleep(3);
  EXPECT_FALSE(queue->read(read_content));
}

TEST(FileWatcherTest, Basics) {
  const string file_name = "./file_watcher_test_file";
  auto watcher = FileWatcher::Instance();

  // Could not remove files not being watched
  EXPECT_FALSE(watcher->RemoveFile(file_name));

  RemoveFile(file_name);
  MPMCQueue<string> queue(1024);

  // could not add non-existing file
  EXPECT_FALSE(watcher->AddFile(file_name, [&queue] (string content) {
        queue.blockingWrite(move(content));
      }));
  string content;
  EXPECT_FALSE(queue.read(content));

  // add an empety file to be watched
  OverwriteFile(file_name, "");
  EXPECT_TRUE(watcher->AddFile(file_name, [&queue] (string content) {
        queue.blockingWrite(move(content));
      }));

  // Got one empty content string
  EXPECT_TRUE(queue.read(content));
  EXPECT_TRUE(content.empty());
  sleep(3);
  EXPECT_FALSE(queue.read(content));

  // Overwrite with an empty string again, and shouldn't get any new content
  OverwriteAndTest(file_name, "", nullptr, &queue);

  // Overwrite with a non empty string, should get the new content
  string expected_content = "abc";
  OverwriteAndTest(file_name, expected_content, &expected_content, &queue);

  // Overwrite with the same content, shouldn't get any notification
  OverwriteAndTest(file_name, "abc", nullptr, &queue);

  // Overwrite with an empty string, and should get ""
  expected_content = "";
  OverwriteAndTest(file_name, expected_content, &expected_content, &queue);

  // Remove the file, the file is automatically unregistered, so we shouldn't
  // get any notification even after recreating it
  RemoveFile(file_name);
  OverwriteAndTest(file_name, "", nullptr, &queue);
  sleep(3);
  // though we have re-registered it, shouldn't get any notification, because
  // content is the same as before
  OverwriteAndTest(file_name, "", nullptr, &queue);
  expected_content = "123";
  OverwriteAndTest(file_name, expected_content, &expected_content, &queue);

  // Remove the file, Adding it is not allowed, because we have scheduled
  // re-registering
  RemoveFile(file_name);
  EXPECT_FALSE(watcher->AddFile(file_name, [&queue] (string content) {
        queue.blockingWrite(move(content));
      }));
  EXPECT_FALSE(queue.read(content));

  // But we are able to remove it
  EXPECT_TRUE(watcher->RemoveFile(file_name));
}

TEST(FileWatcherTest, Move) {
  const string file_name = "./file_watcher_test_file";
  const string file_name_tmp = "./file_watcher_test_file_tmp";
  string content = "123";
  string content_tmp = "abc";
  OverwriteFile(file_name, content);
  OverwriteFile(file_name_tmp, content_tmp);
  auto watcher = FileWatcher::Instance();
  MPMCQueue<string> queue(1024);

  EXPECT_TRUE(watcher->AddFile(file_name, [&queue] (string content) {
        queue.blockingWrite(move(content));
      }));
  string read_content;
  queue.blockingRead(read_content);
  EXPECT_EQ(read_content, content);
  string cmd = "mv " + file_name_tmp + " " + file_name;
  EXPECT_EQ(system(cmd.c_str()), 0);
  queue.blockingRead(read_content);
  EXPECT_EQ(read_content, content_tmp);

  cmd = "mv " + file_name + " " + file_name_tmp;
  EXPECT_EQ(system(cmd.c_str()), 0);
  sleep(3);
  EXPECT_FALSE(queue.read(read_content));
  OverwriteAndTest(file_name, content, &content, &queue);
  EXPECT_TRUE(watcher->RemoveFile(file_name));

  RemoveFile(file_name);
  RemoveFile(file_name_tmp);
}

TEST(FileWatcherTest, MultiFiles) {
  const string file_name_1 = "./file_watcher_test_file_1";
  const string file_name_2 = "./file_watcher_test_file_2";
  auto watcher = FileWatcher::Instance();
  EXPECT_FALSE(watcher->RemoveFile(file_name_1));
  EXPECT_FALSE(watcher->RemoveFile(file_name_2));
  RemoveFile(file_name_1);
  RemoveFile(file_name_2);
  EXPECT_FALSE(watcher->AddFile(file_name_1, [] (string content) {}));
  EXPECT_FALSE(watcher->AddFile(file_name_2, [] (string content) {}));

  string content_1 = "1";
  string content_2 = "2";
  OverwriteFile(file_name_1, content_1);
  OverwriteFile(file_name_2, content_2);
  MPMCQueue<string> queue_1(1024);
  MPMCQueue<string> queue_2(1024);
  EXPECT_TRUE(watcher->AddFile(file_name_1, [&queue_1] (string content) {
        queue_1.blockingWrite(move(content));
      }));
  EXPECT_TRUE(watcher->AddFile(file_name_2, [&queue_2] (string content) {
        queue_2.blockingWrite(move(content));
      }));
  string content;
  queue_1.blockingRead(content);
  EXPECT_EQ(content, content_1);
  queue_2.blockingRead(content);
  EXPECT_EQ(content, content_2);

  EXPECT_TRUE(watcher->RemoveFile(file_name_1));
  OverwriteAndTest(file_name_1, "", nullptr, &queue_1);

  content_2 = "22222";
  OverwriteAndTest(file_name_2, content_2, &content_2, &queue_2);
  RemoveFile(file_name_1);
  RemoveFile(file_name_2);
}

TEST(FileWatcherTest, FullChangeOnly) {
  const string file_name = "./file_watcher_test_file";
  auto watcher = FileWatcher::Instance();
  RemoveFile(file_name);
  MPMCQueue<string> queue(1024);
  string expected_content = "abc";
  OverwriteFile(file_name, expected_content);

  EXPECT_TRUE(watcher->AddFile(file_name, [&queue] (string content) {
        queue.blockingWrite(move(content));
      }));
  string content;
  queue.blockingRead(content);
  EXPECT_EQ(content, expected_content);

  ofstream os(file_name);
  EXPECT_TRUE(os);
  expected_content += "def";
  os << expected_content;
  os.flush();
  expected_content += "ghi";
  os << "ghi";
  os.flush();
  sleep(3);
  EXPECT_FALSE(queue.read(content));
  os.close();
  queue.blockingRead(content);
  EXPECT_EQ(content, expected_content);
  EXPECT_FALSE(queue.read(content));
  RemoveFile(file_name);
}

int main(int argc, char** argv) {
  // FLAGS_recheck_removed_file_interval_ms = 500;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

