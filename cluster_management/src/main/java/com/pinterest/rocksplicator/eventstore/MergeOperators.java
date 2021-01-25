package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  static class BatchLeaderEventMergeOperator
      implements BatchMergeOperator<LeaderEventsHistory> {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(BatchLeaderEventMergeOperator.class);
    final String resourcName;
    final String partitionName;
    final Optional<Integer> maxEventsToKeep;

    public BatchLeaderEventMergeOperator(
        final String resourcName,
        final String partitionName,
        final Optional<Integer> maxEventsToKeep) {
      this.resourcName = resourcName;
      this.partitionName = partitionName;
      this.maxEventsToKeep = maxEventsToKeep;
    }

    @Override
    public LeaderEventsHistory apply(LeaderEventsHistory oldHistory,
                                     LeaderEventsHistory newHistory) {
      List<LeaderEvent>
          events =
          new ArrayList<>(oldHistory.getEventsSize() + newHistory.getEventsSize());

      if (newHistory.getEventsSize() > 0) {
        events.addAll(newHistory.getEvents());
      }
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


  static class SingleLeaderEventMergeOperator
      implements SingleMergeOperator<LeaderEventsHistory, LeaderEvent> {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(SingleLeaderEventMergeOperator.class);
    final String resourcName;
    final String partitionName;
    final Optional<Integer> maxEventsToKeep;

    public SingleLeaderEventMergeOperator(
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
}
