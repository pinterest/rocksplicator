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
