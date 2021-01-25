package com.pinterest.rocksplicator.eventstore;

import static org.junit.Assert.assertEquals;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

public class BatchLeaderEventMergeOperatorTest {

  private BatchLeaderEventMergeOperator operator;
  private Optional<Integer> maxToKeep = Optional.of(3);

  @Before
  public void setUp() {
    operator = new BatchLeaderEventMergeOperator("myResource", "myResource_0", maxToKeep);
  }

  @Test
  public void testBatchMerge() {
    LeaderEventsHistory oldHistory = new LeaderEventsHistory();

    long now = System.currentTimeMillis();

    LeaderEvent leaderEvent_1 = new LeaderEvent().setEvent_timestamp_ms(++now)
        .setEvent_type(LeaderEventType.PARTICIPANT_LEADER_DOWN_INIT)
        .setOriginating_node("localhost");

    LeaderEvent leaderEvent_2 = new LeaderEvent().setEvent_timestamp_ms(++now)
        .setEvent_type(LeaderEventType.PARTICIPANT_LEADER_DOWN_SUCCESS)
        .setOriginating_node("localhost");

    LeaderEventsHistory mergedHistory = operator.apply(oldHistory,
        new LeaderEventsHistory().setEvents(ImmutableList.of(leaderEvent_1, leaderEvent_2)));

    // Events are merged and sorted.
    assertEquals(ImmutableList.of(leaderEvent_2, leaderEvent_1), mergedHistory.getEvents());

    LeaderEvent leaderEvent_3 = new LeaderEvent().setEvent_timestamp_ms(++now)
        .setEvent_type(LeaderEventType.SPECTATOR_OBSERVED_LEADER_UP)
        .setOriginating_node("localhost");

    LeaderEvent leaderEvent_4 = new LeaderEvent().setEvent_timestamp_ms(++now)
        .setEvent_type(LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_UP)
        .setOriginating_node("localhost");

    mergedHistory = operator.apply(mergedHistory,
        new LeaderEventsHistory().setEvents(ImmutableList.of(leaderEvent_3, leaderEvent_4)));

    // events are merged with previous ones, sorted and pruned to keep only last 3 events.
    assertEquals(ImmutableList.of(leaderEvent_4, leaderEvent_3, leaderEvent_2),
        mergedHistory.getEvents());

    LeaderEvent leaderEvent_5 = new LeaderEvent().setEvent_timestamp_ms(++now)
        .setEvent_type(LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_UP)
        .setOriginating_node("localhost");
    mergedHistory = operator.apply(mergedHistory,
        new LeaderEventsHistory().setEvents(ImmutableList.of(leaderEvent_5)));

    // events are merged with previous ones, sorted and pruned to keep only last 3 events.
    assertEquals(ImmutableList.of(leaderEvent_5, leaderEvent_4, leaderEvent_3),
        mergedHistory.getEvents());
  }
}
