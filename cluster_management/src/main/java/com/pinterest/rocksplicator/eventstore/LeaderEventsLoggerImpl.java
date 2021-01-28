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
  private final String instanceId;
  private final boolean isEnabled;

  public LeaderEventsLoggerImpl(
      final String instanceId,
      final String zkConnectString,
      final String clusterName,
      final String resourcesEnabledConfigPath,
      final String resourcesEnabledConfigType) {
    this.instanceId = instanceId;
    this.zkConnectString = zkConnectString;
    this.clusterName = clusterName;
    this.resourcesEnabledConfigPath = resourcesEnabledConfigPath;
    this.resourcesEnabledConfigType = resourcesEnabledConfigType;

    Decoder<byte[], Set<String>> decoder = null;
    try {
      decoder = ConfigCodecs.getDecoder(resourcesEnabledConfigType);
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }

    ConfigStore<Set<String>> localConfigStore = null;
    if (decoder != null) {
      try {
        localConfigStore = new ConfigStore<Set<String>>(decoder, resourcesEnabledConfigPath);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    this.configStore = localConfigStore;

    if (this.configStore != null) {
      this.leaderEventHistoryStore = new LeaderEventHistoryStore(
          zkConnectString, clusterName, Optional.empty());
    } else {
      this.leaderEventHistoryStore = null;
    }

    this.isEnabled = (this.leaderEventHistoryStore != null);
  }

  public boolean isEnabled() {
    return isEnabled;
  }


  public LeaderEventsCollector newEventsCollector(String resourceName, String partitionName) {
    if (isEnabled() && configStore.get().contains(resourceName)) {
      return new LeaderEventsCollectorImpl(resourceName, partitionName);
    } else {
      if (isEnabled()) {
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

    public LeaderEventsCollector addEvent(LeaderEventType eventType, String leaderNode) {
      Preconditions.checkNotNull(eventType);
      if (LeaderEventTypes.participantEventTypes.contains(eventType)) {
        Preconditions.checkArgument(leaderNode == null);
      }
      if (!committed.getAndSet(true)) {
        LeaderEvent leaderEvent = new LeaderEvent();
        leaderEvent.setEvent_timestamp_ms(System.currentTimeMillis())
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
