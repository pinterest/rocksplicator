/// Copyright 2019 Pinterest Inc.
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

#include <string>
#include <utility>
#include <vector>

#include "common/hot_key_detector.h"
#include "gtest/gtest.h"

using common::HotKeyDetector;

std::vector<std::string> generateKeys(const std::string& hot_key,
                                      int percents) {
  std::vector<std::string> keys;
  for (int i = 0; i < 1000 - 10 * percents; ++i) {
    keys.push_back(std::to_string(i));
  }

  for (int i = 0; i < 10 * percents; ++i) {
    keys.push_back(hot_key);
  }

  std::random_shuffle(keys.begin(), keys.end());

  return keys;
}

void testEvenLoad(int load) {
  static const int kRounds = 10;
  for (int percents = 10; percents < 95; ++percents) {
    HotKeyDetector<std::string> detector;
    auto keys = generateKeys("hot", percents);
    for (int i = 0; i < kRounds; ++i) {
      for (const auto key : keys) {
        detector.record(key, load);
      }

      EXPECT_TRUE(detector.isAbove("hot", percents - 1));
      EXPECT_FALSE(detector.isAbove("hot", percents + 1));
      auto hot_keys = detector.getKeysAbove(percents - 1);
      EXPECT_EQ(hot_keys, std::vector<std::string>({"hot"}));
      std::random_shuffle(keys.begin(), keys.end());
    }
  }
}

TEST(HotKeyDetectorTest, EvenLoadTest) {
  testEvenLoad(1);
  testEvenLoad(10);
  testEvenLoad(999);
}

std::vector<std::pair<std::string, uint64_t>>
generateKeyLoad(const std::string& hot_key,
                int percents,
                int hot_key_frequency) {
  std::vector<std::pair<std::string, uint64_t>> keyload;

  for (int i = 0; i < 1000 - 10 * percents; ++i) {
    keyload.push_back(std::make_pair(std::to_string(i), 1));
  }

  for (int i = 0; i < hot_key_frequency; ++i) {
    keyload.push_back(std::make_pair(hot_key,
                                     10 * percents / hot_key_frequency));
  }

  std::random_shuffle(keyload.begin(), keyload.end());

  return keyload;
}

void testUnevenLoad(int percents, int hot_key_frequency) {
  static const int kRounds = 10;

  HotKeyDetector<std::string> detector;
  auto keyload = generateKeyLoad("hot", percents, hot_key_frequency);

  // need to warm up, as the number of non hot key is way more than the hot key.
  for (int i = 0; i < kRounds; ++i) {
    for (const auto kl : keyload) {
      detector.record(kl.first, kl.second);
    }

    std::random_shuffle(keyload.begin(), keyload.end());
  }

  for (int i = 0; i < kRounds; ++i) {
    for (const auto kl : keyload) {
      detector.record(kl.first, kl.second);
    }

    EXPECT_TRUE(detector.isAbove("hot", percents - 1));
    EXPECT_FALSE(detector.isAbove("hot", percents + 1));
    auto hot_keys = detector.getKeysAbove(percents - 1);
    EXPECT_EQ(hot_keys, std::vector<std::string>({"hot"}));
    std::random_shuffle(keyload.begin(), keyload.end());
  }
}

TEST(HotKeyDetectorTest, UnevenLoadTest) {
  for (int percents = 10; percents < 95; ++percents) {
    testUnevenLoad(percents, 2);
    testUnevenLoad(percents, 5);
    testUnevenLoad(percents, 10);
  }
}

void testDecay() {
  HotKeyDetector<std::string> detector;
  int percents = 20;
  auto keys = generateKeys("hot", percents);

  for (int i = 0; i < 2; ++i) {
    for (const auto key : keys) {
      detector.record(key);
    }

    EXPECT_TRUE(detector.isAbove("hot", percents - 1));
    EXPECT_FALSE(detector.isAbove("hot", percents + 1));
    auto hot_keys = detector.getKeysAbove(percents - 1);
    EXPECT_EQ(hot_keys, std::vector<std::string>({"hot"}));
    std::random_shuffle(keys.begin(), keys.end());
  }

  sleep(3);
  // now the percentage of hot key in the new stream drops to 1 percent
  keys = generateKeys("hot", 1);
  for (const auto key : keys) {
    detector.record(key);
  }

  EXPECT_TRUE(detector.isAbove("hot", percents / 2 - 1));
  EXPECT_FALSE(detector.isAbove("hot", percents / 2 + 1));
  auto hot_keys = detector.getKeysAbove(percents / 2 - 1);
  EXPECT_EQ(hot_keys, std::vector<std::string>({"hot"}));
}

void testNoDecay() {
  static const int kRounds = 3;

  HotKeyDetector<std::string> detector;
  int percents = 20;
  auto keys = generateKeys("hot", percents);
  for (int i = 0; i < kRounds; ++i) {
    for (const auto key : keys) {
      detector.record(key);
    }

    EXPECT_TRUE(detector.isAbove("hot", percents - 1));
    EXPECT_FALSE(detector.isAbove("hot", percents + 1));
    auto hot_keys = detector.getKeysAbove(percents - 1);
    EXPECT_EQ(hot_keys, std::vector<std::string>({"hot"}));
    std::random_shuffle(keys.begin(), keys.end());
    sleep(3);
  }
}

TEST(HotKeyDetectorTest, DecayTest) {
  FLAGS_hot_key_detector_decay_time_seconds = 2;
  testDecay();
  testNoDecay();
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
