package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class LeaderEventTypes {
  private LeaderEventTypes() {}

  public static final Set<LeaderEventType> spectatorEventTypes = ImmutableSet.<LeaderEventType>builder()
      .add(LeaderEventType.SPECTATOR_OBSERVED_LEADER_DOWN)
      .add(LeaderEventType.SPECTATOR_OBSERVED_LEADER_UP)
      .add(LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_DOWN)
      .add(LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_UP)
      .build();

  public static final Set<LeaderEventType> clientEventTypes = ImmutableSet.<LeaderEventType>builder()
      .add(LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_DOWN)
      .add(LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_UP)
      .build();

  public static final Set<LeaderEventType> participantEventTypes = ImmutableSet.<LeaderEventType>builder()
      .add(LeaderEventType.PARTICIPANT_LEADER_DOWN_INIT)
      .add(LeaderEventType.PARTICIPANT_LEADER_DOWN_SUCCESS)
      .add(LeaderEventType.PARTICIPANT_LEADER_DOWN_FAILURE)
      .add(LeaderEventType.PARTICIPANT_LEADER_UP_INIT)
      .add(LeaderEventType.PARTICIPANT_LEADER_UP_SUCCESS)
      .add(LeaderEventType.PARTICIPANT_LEADER_UP_FAILURE)
      .build();
}
