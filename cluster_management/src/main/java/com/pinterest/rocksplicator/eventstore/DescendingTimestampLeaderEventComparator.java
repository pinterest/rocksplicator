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
// @author Gopal Rajpurohit (grajpurohit@pinterest.com)
//

package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;

import java.util.Comparator;

class DescendingTimestampLeaderEventComparator implements Comparator<LeaderEvent> {

  @Override
  public int compare(LeaderEvent first, LeaderEvent second) {
    return -Long
        .compare(first.getEvent_timestamp_ms(), second.getEvent_timestamp_ms());
  }
}
