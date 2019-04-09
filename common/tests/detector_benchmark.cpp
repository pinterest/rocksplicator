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
#include <vector>

#include "common/hot_key_detector.h"
#include "folly/Benchmark.h"

std::vector<int> generateEvenDistributionIntKeys(int nKeys) {
  std::vector<int> keys;
  for (int i = 0; i < nKeys; ++i) {
    keys.push_back(i);
  }

  return keys;
}

BENCHMARK(EvenDistributionRecordInt, n) {
  common::HotKeyDetector<int> detector;
  std::vector<int> keys;
  int idx = 0;
  const int nKeys = 100;

  BENCHMARK_SUSPEND {
    keys = generateEvenDistributionIntKeys(nKeys);
  }

  while (n--) {
    if (idx >= nKeys) {
      idx = 0;
    }

    detector.record(keys[idx++]);
  }
}

BENCHMARK(EvenDistributionIsAboveInt, n) {
  common::HotKeyDetector<int> detector;
  std::vector<int> keys;
  int idx = 0;
  const int nKeys = 100;

  BENCHMARK_SUSPEND {
    keys = generateEvenDistributionIntKeys(nKeys);
    for (const auto key : keys) {
      detector.record(key);
    }
  }

  while (n--) {
    if (idx >= nKeys) {
      idx = 0;
    }

    detector.isAbove(keys[idx++], 20);
  }
}

BENCHMARK(_20PercentDistributionRecordInt, n) {
  common::HotKeyDetector<int> detector;
  std::vector<int> keys;
  const int nKeys = 100;
  int idx = 0;

  BENCHMARK_SUSPEND {
    for (int i = 0; i < nKeys * 20 / 100; ++i) {
      keys.push_back(-1);
    }

    for (int i = 0; i < nKeys * 80 / 100; ++i) {
      keys.push_back(i);
    }

    std::random_shuffle(keys.begin(), keys.end());
  }

  while (n--) {
    if (idx >= nKeys) {
      idx = 0;
    }

    detector.record(keys[idx++]);
  }
}

BENCHMARK(EvenDistributionRecordShortString, n) {
  common::HotKeyDetector<std::string> detector;
  std::vector<std::string> keys;
  const int nKeys = 100;
  int idx = 0;

  BENCHMARK_SUSPEND {
    for (int i = 0; i < nKeys; ++i) {
      keys.push_back(std::to_string(i));
    }
  }

  while (n--) {
    if (idx >= nKeys) {
      idx = 0;
    }

    detector.record(keys[idx++]);
  }
}

BENCHMARK(_20PercentDistributionRecordShortString, n) {
  common::HotKeyDetector<std::string> detector;
  std::vector<std::string> keys;
  const int nKeys = 100;
  int idx = 0;

  BENCHMARK_SUSPEND {
    for (int i = 0; i < nKeys * 20 / 100; ++i) {
      keys.push_back(std::to_string(-1));
    }

    for (int i = 0; i < nKeys * 80 / 100; ++i) {
      keys.push_back(std::to_string(i));
    }

    std::random_shuffle(keys.begin(), keys.end());
  }

  while (n--) {
    if (idx >= nKeys) {
      idx = 0;
    }

    detector.record(keys[idx++]);
  }
}


BENCHMARK(EvenDistributionRecordLongString, n) {
  common::HotKeyDetector<std::string> detector;
  std::vector<std::string> keys;
  const int nKeys = 100;
  const int keyLen = 30;
  int idx = 0;

  BENCHMARK_SUSPEND {
    const std::string chars =
      "abcdefghijklmnaoqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    for (int i = 0; i < nKeys; ++i) {
      std::string tmp;
      for (int j = 0; j < keyLen; ++j) {
        tmp.push_back(chars[rand() % chars.size()]);
      }
      keys.push_back(std::move(tmp));
    }
  }

  while (n--) {
    if (idx >= nKeys) {
      idx = 0;
    }

    detector.record(keys[idx++]);
  }
}

BENCHMARK(_20PercentDistributionRecordLongString, n) {
  common::HotKeyDetector<std::string> detector;
  std::vector<std::string> keys;
  const int nKeys = 100;
  const int keyLen = 30;
  int idx = 0;

  BENCHMARK_SUSPEND {
    const std::string chars =
      "abcdefghijklmnaoqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

    for (int i = 0; i < nKeys * 20 / 100; ++i) {
      keys.push_back("abcdefghijklmnaoqrstuvwxyzABCD");
    }

    for (int i = 0; i < nKeys * 80 / 100; ++i) {
      std::string tmp;
      for (int j = 0; j < keyLen; ++j) {
        tmp.push_back(chars[rand() % chars.size()]);
      }
      keys.push_back(std::move(tmp));
    }

    std::random_shuffle(keys.begin(), keys.end());
  }

  while (n--) {
    if (idx >= nKeys) {
      idx = 0;
    }

    detector.record(keys[idx++]);
  }
}

int main(int argc, char **argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();
}

