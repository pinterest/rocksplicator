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

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class LeaderEventTypes {

  private LeaderEventTypes() {}

  public static final Set<LeaderEventType>
      spectatorEventTypes =
      ImmutableSet.<LeaderEventType>builder()
          .add(LeaderEventType.SPECTATOR_OBSERVED_LEADER_DOWN)
          .add(LeaderEventType.SPECTATOR_OBSERVED_LEADER_UP)
          .add(LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_DOWN)
          .add(LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_UP)
          .build();

  public static final Set<LeaderEventType>
      clientEventTypes =
      ImmutableSet.<LeaderEventType>builder()
          .add(LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_DOWN)
          .add(LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_UP)
          .build();

  public static final Set<LeaderEventType>
      participantEventTypes =
      ImmutableSet.<LeaderEventType>builder()
          .add(LeaderEventType.PARTICIPANT_LEADER_DOWN_INIT)
          .add(LeaderEventType.PARTICIPANT_LEADER_DOWN_SUCCESS)
          .add(LeaderEventType.PARTICIPANT_LEADER_DOWN_FAILURE)
          .add(LeaderEventType.PARTICIPANT_LEADER_UP_INIT)
          .add(LeaderEventType.PARTICIPANT_LEADER_UP_SUCCESS)
          .add(LeaderEventType.PARTICIPANT_LEADER_UP_FAILURE)
          .build();
}
