package com.pinterest.rocksplicator.eventstore;

public interface LeaderEventsLogger {
  LeaderEventsCollector newEventsCollector(String resourceName, String partitionName);
}
