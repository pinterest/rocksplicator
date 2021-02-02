package com.pinterest.rocksplicator.eventstore;

import java.io.Closeable;

public interface LeaderEventsLogger extends Closeable {
  /**
   * Provides a collector of leader events for a given resource and partition.
   * All logically batched events are collected in collector before committing it to
   * backing store.
   */
  LeaderEventsCollector newEventsCollector(final String resourceName, final String partitionName);

  /**
   * True if logging is generally enabled.
   */
  boolean isLoggingEnabled();

  /**
   * True is logging is enabled for a given resource.
   */
  boolean isLoggingEnabledForResource(final String resourceName);
}
