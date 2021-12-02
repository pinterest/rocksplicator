/// Copyright 2021 Pinterest Inc.
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
// @author prem (prem@pinterest.com)
//

#include <folly/io/async/EventBase.h>
#include <mutex>
#include <thread>


namespace replicator {
  /**
   * @brief 
   * Validates the upstreams of followers who are not replicating
   */
  class UpstreamValidator {
   public:
    UpstreamValidator();
    void stopAndWait();

    // add a follower
    bool add(const std::string& name);

    // remove a follower
    bool remove(const std::string& name);

   private:
    struct State {
      uint64_t seq_no = 0;
      uint64_t lastmod = 0;
      uint64_t reset = 0;

      void setSequenceNo(uint64_t seq_no, uint64_t lastmod);
    };

    void scheduleValidation();
    void validate();
    std::thread thread_;

    // map of follower db names to state
    std::map<std::string, State> followers;
    std::mutex mutex_;

    // to keep track of how many times validation has run
    uint64_t runCount;
    folly::EventBase evb_;
  };
}