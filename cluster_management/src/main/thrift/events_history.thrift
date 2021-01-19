# Copyright 2016 Pinterest Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

namespace java com.pinterest.rocksplicator.thrift.eventhistory

enum LeaderEventType {
  # All of the event types PREFIXED with PARTICIPANT
  # originates from PARTICIPANT which is an existing leader
  # for a given partition or a PARTICIPANT which is trying
  # to become a new leader.
  PARTICIPANT_LEADER_DOWN_INIT = 1,
  PARTICIPANT_LEADER_DOWN_SUCCESS = 2,
  PARTICIPANT_LEADER_DOWN_FAILURE = 3,
  PARTICIPANT_LEADER_UP_INIT = 4,
  PARTICIPANT_LEADER_UP_SUCCESS = 5,
  PARTICIPANT_LEADER_UP_FAILURE = 6,
  # Following events are currently not used
  # Placeholder events have been placed to
  # help with future migration to new event types
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_07 = 7
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_08 = 8
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_09 = 9
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_10 = 10
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_11 = 11
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_12 = 12
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_13 = 13
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_14 = 14
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_15 = 15
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_16 = 16
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_17 = 17
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_18 = 18
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_19 = 19
  PARTICIPANT_PLACEHOLDER_EVENT_TYPE_20 = 20

  # All of the following events are as observed
  # from a leader spectator instance.
  # spectator instance only logs a specific
  # leader being down or up for the first time
  # it observes the state change for leader.
  SPECTATOR_OBSERVED_LEADER_DOWN = 100,
  SPECTATOR_OBSERVED_LEADER_UP   = 101,
  SPECTATOR_POSTED_SHARDMAP_LEADER_UP = 101,
  SPECTATOR_POSTED_SHARDMAP_LEADER_DOWN = 102,

  # All of the following events are / will be
  # logged from a client that watches the shard_map
  # generated from SPECTATOR. We can use Specator
  # to watch on shard_maps propagation delays from itself.
  CLIENT_OBSERVED_SHARDMAP_LEADER_UP = 200,
  CLIENT_OBSERVED_SHARDMAP_LEADER_DOWN = 201,
}

/**
 * A particular instance of event related to leadership handoff.
 **/
struct LeaderEvent {
  1: optional LeaderEventType event_type,
  # Node that is logging this event
  2: optional string originating_node,
  # If available, which leader node was observed.
  # participant will always be observing itself as leader
  # while performing state transitions. Hence participant
  # can skip populating it, as it will be same as
  # originating node.
  #
  3: optional string observed_leader_node,
  # the timestamp in milliseconds of the
  # event as close to  when the event actually was
  # observed for the first time, and it's effect
  # applied. This is not the update time in external
  # storage when the event got logged, since there can
  # potentially be a delay when the event is observed
  # and when it actually got logged
  4: optional i64 event_timestamp_ms,
}

/**
 * History of events kept sorted in event_timestamp_ms order
 **/
struct LeaderEventsHistory {
  1: required i32 max_events_to_keep = 25;
  2: optional list<LeaderEvent> events,
}
