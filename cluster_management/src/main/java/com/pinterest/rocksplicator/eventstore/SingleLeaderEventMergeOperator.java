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
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

class SingleLeaderEventMergeOperator
    implements SingleMergeOperator<LeaderEventsHistory, LeaderEvent> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SingleLeaderEventMergeOperator.class);
  final String resourcName;
  final String partitionName;
  final Optional<Integer> maxEventsToKeep;

  SingleLeaderEventMergeOperator(
      final String resourcName,
      final String partitionName,
      final Optional<Integer> maxEventsToKeep) {
    this.resourcName = resourcName;
    this.partitionName = partitionName;
    this.maxEventsToKeep = maxEventsToKeep;
  }

  @Override
  public LeaderEventsHistory apply(LeaderEventsHistory oldHistory,
                                   LeaderEvent leaderEvent) {
    List<LeaderEvent> events = new ArrayList<>(oldHistory.getEventsSize());
    events.add(leaderEvent);
    if (oldHistory.getEventsSize() > 0) {
      events.addAll(oldHistory.getEvents());
    }
    Collections.sort(events, new DescendingTimestampLeaderEventComparator());

    LeaderEventsHistory mergedHistory = new LeaderEventsHistory();

    int maxEvents = oldHistory.getMax_events_to_keep();
    if (maxEventsToKeep.isPresent()) {
      maxEvents = maxEventsToKeep.get();
    }
    mergedHistory.setMax_events_to_keep(maxEvents);

    if (events.size() > mergedHistory.getMax_events_to_keep()) {
      mergedHistory.setEvents(events.subList(0, mergedHistory.getMax_events_to_keep()));
    } else {
      mergedHistory.setEvents(events);
    }

    LOGGER.debug(String
        .format("resource: %s partitionName: %s, size: %d, ts_ms: %d", resourcName, partitionName,
            mergedHistory.getEventsSize(),
            mergedHistory.getEvents().get(0).getEvent_timestamp_ms()));
    return mergedHistory;
  }
}
