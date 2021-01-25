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

import static com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_DOWN;
import static com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_UP;
import static com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType.SPECTATOR_OBSERVED_LEADER_DOWN;
import static com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType.SPECTATOR_OBSERVED_LEADER_UP;
import static com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_DOWN;
import static com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_UP;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.WrappedDataThriftCodec;
import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.math.IntMath;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

public class LeaderEventHistoryStore implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderEventHistoryStore.class);
  private static final int NUM_PARALLEL_THREADS = 16;

  private final String zkConnectString;
  private final String clusterName;
  private final CuratorFramework zkClient;
  private final Optional<Integer> maxEventsToKeep;
  private final Codec<LeaderEventsHistory, byte[]> leaderEventsHistoryCodec;
  private final List<ExecutorService> executorServices;

  private final LoadingCache<String, LoadingCache<String,
      MergeableReadWriteStore<LeaderEventsHistory, LeaderEvent>>>
      zkStoreCache;

  private final LoadingCache<String, Cache<String, LeaderEventsHistory>> localHistoryCache;

  private final List<Lock> clusterResourcePartitionLock;

  public LeaderEventHistoryStore(
      final String zkConnectString,
      final String clusterName,
      final Optional<Integer> maxEventsToKeep) {
    this.zkConnectString = Preconditions.checkNotNull(zkConnectString);
    this.clusterName = Preconditions.checkNotNull(clusterName);

    this.leaderEventsHistoryCodec = new WrappedDataThriftCodec(
        LeaderEventsHistory.class, SerializationProtocol.COMPACT, CompressionAlgorithm.GZIP);
    this.maxEventsToKeep = maxEventsToKeep;

    this.zkClient =
        CuratorFrameworkFactory.newClient(Preconditions.checkNotNull(this.zkConnectString),
            new ExponentialBackoffRetry(1000, 3));

    this.zkClient.start();
    try {
      this.zkClient.blockUntilConnected(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOGGER.error(String.format("Can't connect to zookeeper: %s", zkConnectString), e);
    }

    this.zkStoreCache = CacheBuilder.newBuilder()
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .removalListener(
            new RemovalListener<String, LoadingCache<String,
                MergeableReadWriteStore<LeaderEventsHistory, LeaderEvent>>>() {
              @Override
              public void onRemoval(
                  RemovalNotification<String, LoadingCache<String,
                      MergeableReadWriteStore<LeaderEventsHistory, LeaderEvent>>> notification) {
                notification.getValue().invalidateAll();
              }
            })
        .build(new CacheLoader<String,
            LoadingCache<String, MergeableReadWriteStore<LeaderEventsHistory, LeaderEvent>>>() {
          @Override
          public LoadingCache<String, MergeableReadWriteStore<LeaderEventsHistory,
              LeaderEvent>> load(
              String resourceName) throws Exception {
            return CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .removalListener(
                    new RemovalListener<String, MergeableReadWriteStore<LeaderEventsHistory,
                        LeaderEvent>>() {
                      @Override
                      public void onRemoval(
                          RemovalNotification<String, MergeableReadWriteStore<LeaderEventsHistory,
                              LeaderEvent>> notification) {
                        try {
                          notification.getValue().close();
                        } catch (IOException e) {
                          LOGGER.warn("Error while closing the zkStore", e);
                        }
                      }
                    })
                .build(new CacheLoader<String,
                    MergeableReadWriteStore<LeaderEventsHistory, LeaderEvent>>() {
                  @Override
                  public MergeableReadWriteStore<LeaderEventsHistory, LeaderEvent> load(
                      String partitionName) throws Exception {
                    return new ZkMergeableEventStore<>(zkClient, clusterName,
                        resourceName, partitionName, leaderEventsHistoryCodec,
                        LeaderEventsHistory::new,
                        MergeOperators
                            .createBatchMergeOperator(resourceName, partitionName,
                                maxEventsToKeep));
                  }
                });
          }
        });

    this.executorServices = new ArrayList<>(NUM_PARALLEL_THREADS);

    for (int i = 0; i < NUM_PARALLEL_THREADS; ++i) {
      this.executorServices.add(Executors.newSingleThreadExecutor());
    }
    this.clusterResourcePartitionLock = new ArrayList<>(128);
    for (int i = 0; i < 128; ++i) {
      this.clusterResourcePartitionLock.add(new ReentrantLock());
    }

    this.localHistoryCache = CacheBuilder.newBuilder()
        .expireAfterAccess(10, TimeUnit.MINUTES)
        .build(new CacheLoader<String, Cache<String, LeaderEventsHistory>>() {
          @Override
          public Cache<String, LeaderEventsHistory> load(String resourceName) throws Exception {
            return CacheBuilder.newBuilder()
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build();
          }
        });

  }

  private static String getFullIdentifier(String cluster, String resource, String partition) {
    StringBuilder builder = new StringBuilder()
        .append(cluster)
        .append('/')
        .append(resource)
        .append('/')
        .append(partition);
    return builder.toString();
  }

  private ExecutorService getExecutorService(String cluster, String resource, String partition) {
    return this.executorServices.get(
        IntMath.mod(getFullIdentifier(cluster, resource, partition).hashCode(),
            this.executorServices.size()));
  }

  private Lock getLock(String cluster, String resource, String partition) {
    return this.clusterResourcePartitionLock.get(
        IntMath.mod(getFullIdentifier(cluster, resource, partition).hashCode(),
            this.clusterResourcePartitionLock.size()));
  }

  public void asyncAppend(String resourceName, String partitionName, LeaderEvent event) {
    List<LeaderEvent> leaderEvents = new ArrayList<>();
    leaderEvents.add(event);
    asyncBatchAppend(resourceName, partitionName, leaderEvents);
  }

  public void asyncBatchAppend(String resourceName, String partitionName,
                               List<LeaderEvent> events) {
    ExecutorService service = getExecutorService(clusterName, resourceName, partitionName);
    service.submit(() -> batchAppend(resourceName, partitionName, events));
  }

  /**
   * Blocks the calling thread. Hence not directly available to the client.
   */
  @VisibleForTesting
  void batchAppend(String resource, String partition, List<LeaderEvent> leaderEvents) {
    Preconditions.checkNotNull(resource);
    Preconditions.checkNotNull(partition);
    Preconditions.checkNotNull(leaderEvents);

    for (LeaderEvent leaderEvent : leaderEvents) {
      Preconditions.checkArgument(leaderEvent.isSetEvent_type());
      Preconditions.checkArgument(leaderEvent.isSetEvent_timestamp_ms());
      Preconditions.checkArgument(leaderEvent.isSetOriginating_node());
    }

    Lock lock = getLock(clusterName, resource, partition);

    lock.lock();
    try {
      LeaderEventsHistory
          cachedHistory =
          localHistoryCache.getUnchecked(resource).getIfPresent(partition);

      if (cachedHistory == null) {
        try {
          cachedHistory = zkStoreCache.getUnchecked(resource).getUnchecked(partition).read();
          localHistoryCache.getUnchecked(resource).put(partition, cachedHistory);
        } catch (IOException exp) {
          // Do nothing. This happens when we are writing for the first time.
        }
      }
      leaderEvents.sort(new DescendingTimestampLeaderEventComparator());

      // Either all of them are logged or none of them are logged...
      // hence, if any event is not eligible,
      // none in the batch are considered to be eligible.
      for (LeaderEvent leaderEvent : leaderEvents) {
        if (!isEligible(cachedHistory, leaderEvent)) {
          return;
        }
      }
      LeaderEventsHistory batchUpdateHistory = new LeaderEventsHistory().setEvents(leaderEvents);

      LeaderEventsHistory mergedHistory =
          zkStoreCache.getUnchecked(resource).getUnchecked(partition)
              .mergeBatch(batchUpdateHistory);
      localHistoryCache.getUnchecked(resource).put(partition, mergedHistory);
    } catch (Throwable e) {
      LOGGER.error(String.format("Error processing leaderevents: %s", leaderEvents), e);
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  boolean isEligible(LeaderEventsHistory lastKnownHistory, LeaderEvent leaderEvent) {
    if (lastKnownHistory != null) {
      if (LeaderEventTypes.spectatorEventTypes.contains(leaderEvent.getEvent_type())) {
        if (!shouldAddSpectatorEvent(lastKnownHistory, leaderEvent)) {
          // No further processing of this event, as it is a duplicate of previous event
          // and the state has not changed from what is previously available.
          return false;
        }
      } else if (LeaderEventTypes.clientEventTypes.contains(leaderEvent.getEvent_type())) {
        if (!shouldAddClientEvent(lastKnownHistory, leaderEvent)) {
          return false;
        }
      } else if (LeaderEventTypes.participantEventTypes.contains(leaderEvent.getEvent_type())) {
        if (!shouldAddParticipantEvent(lastKnownHistory, leaderEvent)) {
          return false;
        }
      } else {
        // Unknown event, log and skip the event. We skip the event here, in order to protect
        // the integrity of the system. We need to know what we are logging, before logging the
        // data into zk store.
        LOGGER.error("Unknown type of leader event, not logging to zookeeper {}", leaderEvent);
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  boolean shouldAddParticipantEvent(LeaderEventsHistory lastKnownHistory,
                                    LeaderEvent currentLeaderEvent) {
    LeaderEventType currentLeaderEventType = currentLeaderEvent.getEvent_type();
    // we log all participant events, as it is rare occurrence when duplicate transitions gets
    // fired.
    // We can revisit this logic if this is found to be more often.
    return true;
  }

  @VisibleForTesting
  private boolean shouldAddClientEvent(LeaderEventsHistory lastKnownHistory,
                                       LeaderEvent currentLeaderEvent) {
    LeaderEventType currentLeaderEventType = currentLeaderEvent.getEvent_type();
    if (currentLeaderEventType == CLIENT_OBSERVED_SHARDMAP_LEADER_UP) {
      for (int i = 0; i < lastKnownHistory.getEventsSize(); ++i) {
        LeaderEvent oldEvent = lastKnownHistory.getEvents().get(i);
        LeaderEventType oldEventType = oldEvent.getEvent_type();
        if (!LeaderEventTypes.clientEventTypes.contains(oldEventType)) {
          continue;
        }
        // continue if the old event originated on different node.
        if (!oldEvent.getOriginating_node().equals(currentLeaderEvent.getOriginating_node())) {
          continue;
        }
        if (oldEventType == CLIENT_OBSERVED_SHARDMAP_LEADER_DOWN) {
          return true;
        } else if (oldEventType == CLIENT_OBSERVED_SHARDMAP_LEADER_UP) {
          if (oldEvent.getObserved_leader_node()
              .equals(currentLeaderEvent.getObserved_leader_node())) {
            // This is a repeat event, hence ignore this event and do not merge.
            return false;
          } else {
            return true;
          }
        }
      }
      return true;
    } else if (currentLeaderEvent.getEvent_type() == CLIENT_OBSERVED_SHARDMAP_LEADER_DOWN) {
      for (int i = 0; i < lastKnownHistory.getEventsSize(); ++i) {
        LeaderEvent oldEvent = lastKnownHistory.getEvents().get(i);
        LeaderEventType oldEventType = oldEvent.getEvent_type();
        if (!LeaderEventTypes.clientEventTypes.contains(oldEventType)) {
          continue;
        }
        // continue if the old event originated on different node.
        if (!oldEvent.getOriginating_node().equals(currentLeaderEvent.getOriginating_node())) {
          continue;
        }
        if (oldEventType == CLIENT_OBSERVED_SHARDMAP_LEADER_DOWN) {
          // This is a repeat event for downed leader of a partition.. ignore.
          // We won't have an observer_node in this case.
          return false;
        } else if (oldEventType == CLIENT_OBSERVED_SHARDMAP_LEADER_UP) {
          return true;
        }
      }
      // This is a first event of type leader going down, so log it.
      return true;
    }
    LOGGER.error("Unknown event: skipping %s", currentLeaderEvent);
    return false;
  }

  @VisibleForTesting
  private boolean shouldAddSpectatorEvent(LeaderEventsHistory lastKnownHistory,
                                          LeaderEvent currentLeaderEvent) {
    LeaderEventType currentLeaderEventType = currentLeaderEvent.getEvent_type();
    if (currentLeaderEventType == SPECTATOR_OBSERVED_LEADER_UP) {
      for (int i = 0; i < lastKnownHistory.getEventsSize(); ++i) {
        LeaderEvent oldEvent = lastKnownHistory.getEvents().get(i);
        LeaderEventType oldEventType = oldEvent.getEvent_type();
        if (!LeaderEventTypes.spectatorEventTypes.contains(oldEventType)) {
          continue;
        }
        // continue if the old event originated on different node.
        if (!oldEvent.getOriginating_node().equals(currentLeaderEvent.getOriginating_node())) {
          continue;
        }
        if (oldEventType == SPECTATOR_OBSERVED_LEADER_DOWN) {
          return true;
        } else if (oldEventType == SPECTATOR_OBSERVED_LEADER_UP) {
          if (oldEvent.getObserved_leader_node()
              .equals(currentLeaderEvent.getObserved_leader_node())) {
            // This is a repeat event, hence ignore this event and do not merge.
            return false;
          } else {
            return true;
          }
        }
      }
      return true;
    } else if (currentLeaderEventType == SPECTATOR_POSTED_SHARDMAP_LEADER_UP) {
      for (int i = 0; i < lastKnownHistory.getEventsSize(); ++i) {
        LeaderEvent oldEvent = lastKnownHistory.getEvents().get(i);
        LeaderEventType oldEventType = oldEvent.getEvent_type();
        if (!LeaderEventTypes.spectatorEventTypes.contains(oldEventType)) {
          continue;
        }
        // continue if the old event originated on different node.
        if (!oldEvent.getOriginating_node().equals(currentLeaderEvent.getOriginating_node())) {
          continue;
        }
        if (oldEventType == SPECTATOR_POSTED_SHARDMAP_LEADER_DOWN) {
          return true;
        } else if (oldEventType == SPECTATOR_POSTED_SHARDMAP_LEADER_UP) {
          if (oldEvent.getObserved_leader_node()
              .equals(currentLeaderEvent.getObserved_leader_node())) {
            // This is a repeat event, hence ignore this event and do not merge.
            return false;
          } else {
            return true;
          }
        }
      }
      return true;
    } else if (currentLeaderEvent.getEvent_type() == SPECTATOR_OBSERVED_LEADER_DOWN) {
      for (int i = 0; i < lastKnownHistory.getEventsSize(); ++i) {
        LeaderEvent oldEvent = lastKnownHistory.getEvents().get(i);
        LeaderEventType oldEventType = oldEvent.getEvent_type();
        if (!LeaderEventTypes.spectatorEventTypes.contains(oldEventType)) {
          continue;
        }
        // continue if the old event originated on different node.
        if (!oldEvent.getOriginating_node().equals(currentLeaderEvent.getOriginating_node())) {
          continue;
        }
        if (oldEventType == SPECTATOR_OBSERVED_LEADER_DOWN) {
          // This is a repeat event for downed leader of a partition.. ignore.
          // We won't have an observer_node in this case.
          return false;
        } else if (oldEventType == SPECTATOR_OBSERVED_LEADER_UP) {
          return true;
        }
      }
      // This is a first event of type leader going down, so log it.
      return true;
    } else if (currentLeaderEvent.getEvent_type() == SPECTATOR_POSTED_SHARDMAP_LEADER_DOWN) {
      for (int i = 0; i < lastKnownHistory.getEventsSize(); ++i) {
        LeaderEvent oldEvent = lastKnownHistory.getEvents().get(i);
        LeaderEventType oldEventType = oldEvent.getEvent_type();
        if (!LeaderEventTypes.spectatorEventTypes.contains(oldEventType)) {
          continue;
        }
        // continue if the old event originated on different node.
        if (!oldEvent.getOriginating_node().equals(currentLeaderEvent.getOriginating_node())) {
          continue;
        }
        if (oldEventType == SPECTATOR_POSTED_SHARDMAP_LEADER_DOWN) {
          // This is a repeat event for downed leader of a partition.. ignore.
          // We won't have an observer_node in this case.
          return false;
        } else if (oldEventType == SPECTATOR_POSTED_SHARDMAP_LEADER_UP) {
          return true;
        }
      }
      // This is a first event of type leader going down, so log it.
      return true;
    }
    LOGGER.error("Unknown event: skipping %s", currentLeaderEvent);
    return false;
  }

  @Override
  public void close() throws IOException {
    for (ExecutorService service : executorServices) {
      service.shutdown();
    }
    for (ExecutorService service : executorServices) {
      while (!service.isTerminated()) {
        try {
          service.awaitTermination(1000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOGGER.error("Interrupted: ", e);
        }
      }
    }
    this.zkStoreCache.asMap().forEach(
        new BiConsumer<String, LoadingCache<String, MergeableReadWriteStore<LeaderEventsHistory,
            LeaderEvent>>>() {
          @Override
          public void accept(String resourceName,
                             LoadingCache<String, MergeableReadWriteStore<LeaderEventsHistory,
                                 LeaderEvent>> cache) {
            cache.asMap().forEach(
                new BiConsumer<String, MergeableReadWriteStore<LeaderEventsHistory, LeaderEvent>>() {
                  @Override
                  public void accept(String partitionName,
                                     MergeableReadWriteStore<LeaderEventsHistory, LeaderEvent> zkStore) {
                    try {
                      zkStore.close();
                    } catch (IOException e) {
                      LOGGER.error(String
                          .format("Couldn't close zkStore for resource: %s partition: %s",
                              resourceName, partitionName));
                    }
                  }
                });
            cache.invalidateAll();
          }
        });
    this.zkStoreCache.invalidateAll();

    this.zkClient.close();
  }
}
