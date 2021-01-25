package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;

import java.util.Comparator;

/**
 *
 */
class DescendingTimestampLeaderEventComparator implements Comparator<LeaderEvent> {
  @Override
  public int compare(LeaderEvent first, LeaderEvent second) {
    return -Long
        .compare(first.getEvent_timestamp_ms(), second.getEvent_timestamp_ms());
  }
}
