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
  # Next new enum value = 21
  # Next placeholder enum value to use : 13
  PARTICIPANT_LEADER_DOWN_INIT = 1,
  PARTICIPANT_LEADER_DOWN_SUCCESS = 2,
  PARTICIPANT_LEADER_DOWN_FAILURE = 3,
  PARTICIPANT_LEADER_UP_INIT = 4,
  PARTICIPANT_LEADER_UP_SUCCESS = 5,
  PARTICIPANT_LEADER_UP_FAILURE = 6,
  SPECTATOR_OBSERVED_LEADER_DOWN = 7,
  SPECTATOR_OBSERVED_LEADER_UP   = 8,
  SPECTATOR_POSTED_SHARDMAP_LEADER_UP = 9,
  SPECTATOR_POSTED_SHARDMAP_LEADER_DOWN = 10,
  CLIENT_OBSERVED_SHARDMAP_LEADER_UP = 11,
  CLIENT_OBSERVED_SHARDMAP_LEADER_DOWN = 12,

  # Following events are currently not used
  # Placeholder events have been placed to
  # help with future migration to new event types
  PLACEHOLDER_EVENT_TYPE_13 = 13
  PLACEHOLDER_EVENT_TYPE_14 = 14
  PLACEHOLDER_EVENT_TYPE_15 = 15
  PLACEHOLDER_EVENT_TYPE_16 = 16
  PLACEHOLDER_EVENT_TYPE_17 = 17
  PLACEHOLDER_EVENT_TYPE_18 = 18
  PLACEHOLDER_EVENT_TYPE_19 = 19
  PLACEHOLDER_EVENT_TYPE_20 = 20
}

struct LeaderEvent {
  1: optional LeaderEventType event_type,
  2: optional string observer_node,
  3: optional i64 timestamp_ms,
}

struct LeaderEventsHistory {
  1: required i32 max_events_to_keep = 25;
  2: optional list<LeaderEvent> events,
}
