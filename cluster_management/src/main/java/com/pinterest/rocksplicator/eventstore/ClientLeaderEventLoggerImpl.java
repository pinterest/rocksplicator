package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.codecs.SimpleJsonObjectDecoder;
import com.pinterest.rocksplicator.config.ConfigNotifier;
import com.pinterest.rocksplicator.config.FileWatchers;
import com.pinterest.rocksplicator.shardmap.Partition;
import com.pinterest.rocksplicator.shardmap.Replica;
import com.pinterest.rocksplicator.shardmap.ReplicaState;
import com.pinterest.rocksplicator.shardmap.ResourceMap;
import com.pinterest.rocksplicator.shardmap.ShardMap;
import com.pinterest.rocksplicator.shardmap.ShardMaps;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.math.IntMath;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ClientLeaderEventLoggerImpl implements ClientLeaderEventLogger {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ClientLeaderEventLoggerImpl.class);
  private static final int NUM_THREADS = 4;
  private final LeaderEventsLogger leaderEventsLogger;
  private final List<ExecutorService> executorServices;
  private final LoadingCache<String, Cache<String, Leader>> leaderStateCache;

  private final ConfigNotifier<JSONObject> notifier;

  public ClientLeaderEventLoggerImpl(
      final LeaderEventsLogger leaderEventsLogger,
      final String shardMapPath) {

    this.leaderEventsLogger = leaderEventsLogger;

    ImmutableList.Builder<ExecutorService> listBuilder = ImmutableList.builder();
    for (int i = 0; i < NUM_THREADS; ++i) {
      listBuilder.add(Executors.newSingleThreadExecutor());
    }
    this.executorServices = listBuilder.build();

    ConfigNotifier<JSONObject> localNotifier = null;

    if (this.leaderEventsLogger != null && leaderEventsLogger.isLoggingEnabled()) {
      try {
        localNotifier = new ConfigNotifier<>(
            new SimpleJsonObjectDecoder(),
            shardMapPath,
            FileWatchers.getPollingPerSecondFileWatcher(),
            jsonObjectContext -> {
              process(jsonObjectContext);
              return null;
            });
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    this.notifier = localNotifier;

    this.leaderStateCache = CacheBuilder.newBuilder()
        .removalListener((RemovalListener<String, Cache<String, Leader>>)
            notification -> notification.getValue().asMap().clear())
        .build(new CacheLoader<String, Cache<String, Leader>>() {
          @Override
          public Cache<String, Leader> load(String resource) throws Exception {
            return CacheBuilder.newBuilder()
                .build();
          }
        });
  }

  private void process(ConfigNotifier.Context<JSONObject> shardMapWithContext) {
    JSONObject jsonShardMap = shardMapWithContext.getItem();
    ShardMap shardMap = ShardMaps.fromJson(jsonShardMap);
    process(shardMap, shardMapWithContext.getSrc_change_time_millis());
  }

  @Override
  public void process(ShardMap shardMap, long shardMapNotificationTimeMillis) {
    if (leaderEventsLogger == null || !leaderEventsLogger.isLoggingEnabled()) {
      return;
    }

    for (String resourceName : shardMap.getResources()) {
      if (!leaderEventsLogger.isLoggingEnabledForResource(resourceName)) {
        // Do nothing for resource that is not enabled.
        LOGGER.error(String.format(
            "Skipping resource %s because resource is not enabled for LeaderEvents logging",
            resourceName));
        continue;
      }

      ResourceMap resourceMap = shardMap.getResourceMap(resourceName);

      // Here, process each resource in a separate thread.
      executorServices.get(IntMath.mod(resourceName.hashCode(), executorServices.size()));
    }
  }

  @Override
  public void close() throws IOException {
    // First close the notification of any future events.
    this.notifier.close();

    /**
     * Shutdown all executor services to no longer accept new tasks.
     */
    for (ExecutorService service : executorServices) {
      service.shutdown();
    }

    /**
     * Wait for all present tasks to be completed.
     */
    for (ExecutorService service : executorServices) {
      while (!service.isTerminated()) {
        try {
          service.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          LOGGER.error("Interrupted: ", e);
        }
      }
    }
  }

  private class ResourceMapProcessor implements Runnable {

    private ResourceMap resourceMap;
    private long timeOfEventMillis;

    public ResourceMapProcessor(ResourceMap resourceMap, long timeOfEventMillis) {
      this.resourceMap = resourceMap;
      this.timeOfEventMillis = timeOfEventMillis;
    }

    private void runUnprotected() {
      String resourceName = resourceMap.getResource();

      Cache<String, Leader> resourceLeaderCache = leaderStateCache.getUnchecked(resourceName);

      for (Partition partition : resourceMap.getAllPartitions()) {
        List<Replica> replicas = resourceMap.getAllReplicasForPartition(partition);
        if (replicas == null) {
          replicas = ImmutableList.of();
        }

        boolean leaderFound = false;
        Leader previousLeader = resourceLeaderCache.getIfPresent(partition.getPartitionName());
        for (Replica replica : replicas) {
          if (replica.getReplicaState() == ReplicaState.LEADER) {
            leaderFound = true;
            if (previousLeader == null // either if we don't have this partition info cached.
                // or if the previous leader was down.
                || previousLeader.getState() == LeaderState.LEADER_DOWN
                // if we didn't know who the previous leader was
                || previousLeader.getLeaderInstanceId() == null
                // or if the leader changed
                || !previousLeader.getLeaderInstanceId()
                .equalsIgnoreCase(replica.getInstance().getInstanceId())
            ) {
              LOGGER.error(String
                  .format("LEADER_UP (Published) resource:%s, partition=%s, leader:%s",
                      resourceName, partition.getPartitionName(),
                      replica.getInstance().getInstanceId()));
              resourceLeaderCache.put(partition.getPartitionName(),
                  new Leader(replica.getInstance().getInstanceId(), LeaderState.LEADER_UP));
              leaderEventsLogger.newEventsCollector(resourceName, partition.getPartitionName())
                  .addEvent(LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_UP,
                      replica.getInstance().getInstanceId(), timeOfEventMillis);
            } else {
              // Do nothing, as this is potentially a duplicate event.
            }
          }
        }
        if (!leaderFound) {
          // Cache of previous LEADER
          // All replicas are offline. Hence publish an event that this is offline.
          if (previousLeader == null
              || previousLeader.getState() == LeaderState.LEADER_UP) {
            String leaderNode =
                (previousLeader == null || previousLeader.getLeaderInstanceId() == null)
                ? null : previousLeader.getLeaderInstanceId();
            LOGGER.error(String
                .format("LEADER_UP (Published) resource:%s, partition=%s, leader:%s",
                    resourceName, partition.getPartitionName(), leaderNode));
            resourceLeaderCache
                .put(partition.getPartitionName(), new Leader(leaderNode, LeaderState.LEADER_DOWN));
            leaderEventsLogger.newEventsCollector(resourceName, partition.getPartitionName())
                .addEvent(LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_DOWN,
                    leaderNode, timeOfEventMillis)
                .commit();
          } else {
            // Do nothing as this is potentially a duplicate event.
          }
        }
      }
    }

    @Override
    public void run() {
      try {
        runUnprotected();
      } catch (Throwable throwable) {
        LOGGER.error("Error processing client resource map for resource: " + resourceMap.toString(),
            throwable);
      }
    }
  }
}
