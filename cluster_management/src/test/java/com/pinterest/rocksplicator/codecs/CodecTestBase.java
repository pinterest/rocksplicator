package com.pinterest.rocksplicator.codecs;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.junit.After;
import org.junit.Before;

public abstract class CodecTestBase {
  protected LeaderEventsHistory history;

  @Before
  public void setUp() {
    history = new LeaderEventsHistory();
    for (LeaderEventType eventType : LeaderEventType.values()) {
      LeaderEvent event = new LeaderEvent();
      event.setEvent_type(eventType)
          .setEvent_timestamp_ms(System.currentTimeMillis())
          .setOriginating_node("originating_node")
          .setObserved_leader_node("observed_leader_node");
      history.addToEvents(event);
    }
  }

  @After
  public void tearDown() {

  }
}
