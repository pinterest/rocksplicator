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

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import com.google.common.collect.ImmutableList;
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
