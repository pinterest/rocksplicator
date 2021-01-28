package com.pinterest.rocksplicator.eventstore;

import java.io.Closeable;

public interface LeaderEventsLogger extends Closeable {
  LeaderEventsCollector newEventsCollector(String resourceName, String partitionName);
}
