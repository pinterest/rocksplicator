package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.codecs.Decoder;
import com.pinterest.rocksplicator.config.ConfigCodecs;
import com.pinterest.rocksplicator.config.ConfigStore;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class LeaderEventsLoggerImpl implements LeaderEventsLogger {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderEventsLoggerImpl.class);

  private final String zkConnectString;
  private final String clusterName;
  private final String resourcesEnabledConfigPath;
  private final String resourcesEnabledConfigType;
  private final LeaderEventHistoryStore leaderEventHistoryStore;
  private final ConfigStore<Set<String>> configStore;
  private final Optional<Integer> maxEventsToKeep;
  private final String instanceId;
  private final boolean isEnabled;

  public LeaderEventsLoggerImpl(
      final String instanceId,
      final String zkConnectString,
      final String clusterName,
      final String resourcesEnabledConfigPath,
      final String resourcesEnabledConfigType,
      final Optional<Integer> maxEventsToKeep) {
    this.instanceId = instanceId;
    this.zkConnectString = zkConnectString;
    this.clusterName = clusterName;
    this.resourcesEnabledConfigPath = resourcesEnabledConfigPath;
    this.resourcesEnabledConfigType = resourcesEnabledConfigType;
    this.maxEventsToKeep = maxEventsToKeep;

    Decoder<byte[], Set<String>> decoder = null;
    if (resourcesEnabledConfigType != null && !resourcesEnabledConfigType.isEmpty()) {
      try {
        decoder = ConfigCodecs.getDecoder(resourcesEnabledConfigType);
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }

    /**
     * If there is not valid resourceConfigType available, there is no configStore.
     */
    ConfigStore<Set<String>> localConfigStore = null;
    if (decoder != null && resourcesEnabledConfigPath != null && !resourcesEnabledConfigPath.isEmpty()) {
      try {
        localConfigStore = new ConfigStore<Set<String>>(decoder, resourcesEnabledConfigPath);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    this.configStore = localConfigStore;

    /**
     * If there is no configStore or empty zkConnectString, there is no leaderEventHistoryStore
     */
    if (this.configStore != null && zkConnectString != null && !zkConnectString.isEmpty()) {
      this.leaderEventHistoryStore = new LeaderEventHistoryStore(
          zkConnectString, clusterName, maxEventsToKeep);
    } else {
      this.leaderEventHistoryStore = null;
    }

    /**
     * If there is no leaderEventHistoryStore, then the logger implementation is disabled.
     */
    this.isEnabled = (this.leaderEventHistoryStore != null);
  }

  @Override
  public boolean isLoggingEnabled() {
    return isEnabled;
  }

  @Override
  public boolean isLoggingEnabledForResource(final String resourceName) {
    return configStore.get().contains(resourceName);
  }

  @Override
  public LeaderEventsCollector newEventsCollector(
      final String resourceName,
      final String partitionName) {
    if (isLoggingEnabled() && isLoggingEnabledForResource(resourceName)) {
      return new LeaderEventsCollectorImpl(resourceName, partitionName);
    } else {
      if (isLoggingEnabled()) {
        return new ResourceDisabledLeaderEventsCollector(resourceName, partitionName);
      } else {
        return new LoggingDisabledLeaderEventsCollector();
      }
    }
  }

  @Override
  public void close() throws IOException {
    leaderEventHistoryStore.close();
  }

  private static class LoggingDisabledLeaderEventsCollector implements LeaderEventsCollector {
    @Override
    public LeaderEventsCollector addEvent(LeaderEventType eventType, String leaderNode) {
      // Ignore
      return this;
    }

    @Override
    public LeaderEventsCollector addEvent(LeaderEventType eventType, String leaderNode,
                                          long eventTimeMillis) {
      return this;
    }

    @Override
    public void commit() {
      // Ignore
    }
  }

  private static class ResourceDisabledLeaderEventsCollector implements LeaderEventsCollector {
    private final String resourceName;
    private final String partitionName;

    ResourceDisabledLeaderEventsCollector(String resourceName, String partitionName) {
      this.resourceName = resourceName;
      this.partitionName = partitionName;
    }

    @Override
    public LeaderEventsCollector addEvent(LeaderEventType eventType, String leaderNode) {
      LOGGER.info(String.format("Ignoring disabled Resource: %s Partition:%s LeaderEventType: %s", resourceName, partitionName, eventType));
      return this;
    }

    @Override
    public LeaderEventsCollector addEvent(LeaderEventType eventType, String leaderNode,
                                          long eventTimeMillis) {
      LOGGER.info(String.format("Ignoring disabled Resource: %s Partition:%s LeaderEventType: %s", resourceName, partitionName, eventType));
      return this;
    }

    @Override
    public void commit() {
      // Ignore
    }
  }

  private class LeaderEventsCollectorImpl implements LeaderEventsCollector {

    private final LeaderEventsHistory history;
    private final String resourceName;
    private final String partitionName;
    private final AtomicBoolean committed;

    public LeaderEventsCollectorImpl(String resourceName, String partitionName) {
      this.history = new LeaderEventsHistory();
      this.resourceName = resourceName;
      this.partitionName = partitionName;
      this.committed = new AtomicBoolean(false);
    }

    @Override
    public LeaderEventsCollector addEvent(LeaderEventType eventType, String leaderNode) {
      return addEvent(eventType, leaderNode, System.currentTimeMillis());
    }

    @Override
    public LeaderEventsCollector addEvent(LeaderEventType eventType, String leaderNode,
                                          long eventTimeMillis) {
      Preconditions.checkNotNull(eventType);
      if (LeaderEventTypes.participantEventTypes.contains(eventType)) {
        Preconditions.checkArgument(leaderNode == null);
      }

      if (!committed.getAndSet(true)) {
        LeaderEvent leaderEvent = new LeaderEvent();
        leaderEvent.setEvent_timestamp_ms(eventTimeMillis)
            .setEvent_type(eventType)
            .setOriginating_node(instanceId);
        if (leaderNode != null) {
          leaderEvent.setObserved_leader_node(leaderNode);
        }
        history.addToEvents(leaderEvent);
        committed.set(false);
      } else {
        throw new IllegalStateException("Already committed or concurrent modification");
      }
      return this;
    }

    @Override
    public void commit() {
      if (!committed.getAndSet(true)) {
        if (history.getEventsSize() > 0) {
          LeaderEventsLoggerImpl.this.leaderEventHistoryStore
              .asyncBatchAppend(resourceName, partitionName, history.getEvents());
        }
      } else {
        throw new IllegalStateException("Already committed or concurrent modification");
      }
    }
  }
}
