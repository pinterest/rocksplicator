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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.junit.Test;

public class DescendingTimestampLeaderEventComparatorTest {

  @Test
  public void testDescendingOrderSorting() {
    LeaderEventsHistory history = new LeaderEventsHistory();

    LeaderEventType[] leaderEventTypes = LeaderEventType.values();
    long now = System.currentTimeMillis();
    for (int i = 0; i < LeaderEventType.values().length; ++i) {
      LeaderEventType leaderEventType = leaderEventTypes[i % leaderEventTypes.length];
      LeaderEvent leaderEvent = new LeaderEvent()
          .setEvent_type(leaderEventType)
          .setEvent_timestamp_ms(now)
          .setOriginating_node("localhost")
          .setObserved_leader_node("leaderNode");

      history.addToEvents(leaderEvent);

      // The new latest event is appended at the end of the list
      assertEquals(leaderEvent, history.getEvents().get(i));
      history.getEvents().sort(new DescendingTimestampLeaderEventComparator());
      // The new latest event is first in the list after sorting through above comparator.
      assertEquals(leaderEvent, history.getEvents().get(0));

      // The events are always sorted in descending order of the timestamp.
      for (int j = 1; j < history.getEventsSize(); ++j) {
        assertTrue(history.getEvents().get(j).getEvent_timestamp_ms()
            < history.getEvents().get(j - 1).getEvent_timestamp_ms());
      }

      ++now;
    }
  }
}
