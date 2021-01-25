package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import java.util.Optional;

public class MergeOperators {

  private MergeOperators() {}

  public static SingleMergeOperator<LeaderEventsHistory, LeaderEvent> createSingleMergeOperator(
      String resourceName, String partitionName, Optional<Integer> maxEventsToKeep) {
    return new SingleLeaderEventMergeOperator(resourceName, partitionName, maxEventsToKeep);
  }

  public static BatchMergeOperator<LeaderEventsHistory> createBatchMergeOperator(
      String resourceName, String partitionName, Optional<Integer> maxEventsToKeep) {
    return new BatchLeaderEventMergeOperator(resourceName, partitionName, maxEventsToKeep);
  }


}
