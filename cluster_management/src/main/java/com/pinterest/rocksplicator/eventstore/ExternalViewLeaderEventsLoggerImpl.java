package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;
import com.pinterest.rocksplicator.utils.ExternalViewUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.math.IntMath;
import org.apache.helix.model.ExternalView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ExternalViewLeaderEventsLoggerImpl implements ExternalViewLeaderEventLogger {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ExternalViewLeaderEventsLoggerImpl.class);
  private static final int NUM_THREADS = 4;

  private final LeaderEventsLogger leaderEventsLogger;
  private final List<ExecutorService> executorServices;
  private final LoadingCache<String, Cache<String, Leader>> leaderStateCache;

  /**
   * Does not own the LeaderEventsLogger and hence should not close it.
   */
  public ExternalViewLeaderEventsLoggerImpl(
      final LeaderEventsLogger leaderEventsLogger) {
    this.leaderEventsLogger = leaderEventsLogger;

    ImmutableList.Builder<ExecutorService> listBuilder = ImmutableList.builder();
    for (int i = 0; i < NUM_THREADS; ++i) {
      listBuilder.add(Executors.newSingleThreadExecutor());
    }
    this.executorServices = listBuilder.build();

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

  /**
   * Process Leader Events in separate function.
   * Only pass in the externalView if the
   * stateModel is one which has a leader.
   */
  public synchronized void process(
      final List<ExternalView> externalViews,
      final Set<String> disabledHosts,
      final long shardMapGenStartTimeMillis,
      final long shardMapPostTimeMillis) {
    if (leaderEventsLogger == null || !leaderEventsLogger.isLoggingEnabled()) {
      // Do nothing here.
      LOGGER.error("LeaderEventLogger is null or not enabled");
      return;
    }

    Set<String> immutableDisabledHosts = null;

    for (ExternalView externalView : externalViews) {
      if (externalView == null) {
        continue;
      }
      String resourceName = externalView.getResourceName();
      if (resourceName == null) {
        continue;
      }
      if (!leaderEventsLogger.isLoggingEnabledForResource(resourceName)) {
        // Do nothing for resource that is not enabled.
        LOGGER.error(String.format(
            "Skipping resource %s because resource is not enabled for LeaderEvents logging",
            resourceName));
        if (this.leaderStateCache.getIfPresent(resourceName) != null) {
          this.leaderStateCache.invalidate(resourceName);
        }
        continue;
      }

      if (!ExternalViewUtils.isReadWriteStateModel(externalView)) {
        LOGGER.error(String.format(
            "Skipping resource %s because resource is not valid StateModel", resourceName));
        continue;
      }

      if (immutableDisabledHosts == null) {
        immutableDisabledHosts = ImmutableSet.copyOf(disabledHosts);
      }

      executorServices.get(IntMath.mod(resourceName.hashCode(), executorServices.size()))
          .submit(new ExternalViewProcessor(
              externalView, immutableDisabledHosts, shardMapGenStartTimeMillis,
              shardMapPostTimeMillis));
    }
  }

  @Override
  public synchronized void close() throws IOException {
    for (ExecutorService service : executorServices) {
      service.shutdown();
    }
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

  private class ExternalViewProcessor implements Runnable {

    private final ExternalView externalView;
    private final long shardMapGenStartTimeMillis;
    private final long shardMapPostTimeMillis;
    private final Set<String> disabledHosts;

    public ExternalViewProcessor(
        final ExternalView externalView,
        final Set<String> disabledHosts,
        final long shardMapGenStartTimeMillis,
        final long shardMapPostTimeMillis) {
      this.externalView = externalView;
      this.disabledHosts = disabledHosts;
      this.shardMapGenStartTimeMillis = shardMapGenStartTimeMillis;
      this.shardMapPostTimeMillis = shardMapPostTimeMillis;
    }

    private void publishStatus(
        final String resourceName,
        final String partitionName,
        final String host,
        final LeaderState state) {
      LeaderEventsCollector
          collector =
          leaderEventsLogger.newEventsCollector(resourceName, partitionName);
      switch (state) {
        case LEADER_UP:
          collector =
              collector.addEvent(LeaderEventType.SPECTATOR_OBSERVED_LEADER_UP, host,
                  shardMapGenStartTimeMillis);
          if (shardMapPostTimeMillis > 0) {
            collector =
                collector.addEvent(LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_UP, host,
                    shardMapPostTimeMillis);
          }
          break;
        case LEADER_DOWN:
          collector =
              collector.addEvent(LeaderEventType.SPECTATOR_OBSERVED_LEADER_DOWN, host,
                  shardMapGenStartTimeMillis);
          if (shardMapPostTimeMillis > 0) {
            collector =
                collector.addEvent(LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_DOWN, host,
                    shardMapPostTimeMillis);
          }
          break;
      }
      collector.commit();
    }

    private void doNothing() {
      return;
    }

    @Override
    public void run() {
      try {
        runUnprotected();
      } catch (Throwable e) {
        LOGGER.error("ExternalView processing error: ", e);
      }
    }

    private void runUnprotected() {
      String resourceName = externalView.getResourceName();
      Set<String> partitions = externalView.getPartitionSet();

      if (partitions == null || partitions.size() == 0) {
        return;
      }

      Cache<String, Leader> resourceLeaderCache = leaderStateCache.getUnchecked(resourceName);
      for (String partitionName : partitions) {
        Leader previouslyKnownLeader = resourceLeaderCache.getIfPresent(partitionName);
        Map<String, String> hostToState = externalView.getStateMap(partitionName);
        // We don't know who was the leader that went down...
        // all we know is that a leader is not available for
        // this partition, unless we find a host with leader state.
        boolean leaderFound = false;
        for (Map.Entry<String, String> entry : hostToState.entrySet()) {
          String host = entry.getKey();
          String state = entry.getValue();

          if (ExternalViewUtils.StateUtils.isStateAnyKindOfLeader(state)) {
            //Since this host is disabled, whatever is the state, we consider this to be down
            leaderFound = true;
            if (disabledHosts.contains(host)) {
              if (previouslyKnownLeader == null
                  || previouslyKnownLeader.getState() == LeaderState.LEADER_UP
                  || (previouslyKnownLeader.getState() == LeaderState.LEADER_DOWN
                          && (previouslyKnownLeader.getLeaderInstanceId() == null || !host
                  .equals(previouslyKnownLeader.getLeaderInstanceId())))) {
                /**
                 * We publish the event as LEADER_DOWN if
                 * either previously there was no information about this partition
                 * or previously the known leader was up
                 * or previously the leader host was not known
                 * or if previous leader was known, it has changed now.
                 */
                LOGGER.error(String
                    .format("LEADER_DOWN (Published) resource:%s, partition=%s, leader:%s",
                        resourceName, partitionName, host));
                resourceLeaderCache.put(partitionName, new Leader(host, LeaderState.LEADER_DOWN));
                publishStatus(resourceName, partitionName, host, LeaderState.LEADER_DOWN);
              } else {
                // Avoid posting duplicate event.
                LOGGER.error(String
                    .format("LEADER_DOWN (Duplicate/Skipped) resource:%s, partition=%s, leader:%s",
                        resourceName, partitionName, host));
                doNothing();
              }
            } else {
              if (previouslyKnownLeader == null
                  || previouslyKnownLeader.getState() == LeaderState.LEADER_DOWN
                  || (previouslyKnownLeader.getState() == LeaderState.LEADER_UP
                          && (previouslyKnownLeader.getLeaderInstanceId() == null || !host
                  .equals(previouslyKnownLeader.getLeaderInstanceId())))) {
                /**
                 * We publish the event as LEADER_DOWN if
                 * either previously there was no information about this partition
                 * or previously the known leader was up
                 * or previously the leader host was not known
                 * or if previous leader was known, it has changed now.
                 */
                LOGGER.error(String
                    .format("LEADER_UP (Published) resource:%s, partition=%s, leader:%s",
                        resourceName, partitionName, host));
                resourceLeaderCache.put(partitionName, new Leader(host, LeaderState.LEADER_UP));
                publishStatus(resourceName, partitionName, host, LeaderState.LEADER_UP);
              } else {
                // Avoid posting duplicate event.
                LOGGER.error(String
                    .format("LEADER_UP (Duplicate/Skipped) resource:%s, partition=%s, leader:%s",
                        resourceName, partitionName, host));
                doNothing();
              }
            }
          }
        }
        if (!leaderFound) {
          // There is no leader found for this partition. Hence, it is considered to be down.
          // However, check if this leader was previously noted to be down. If so, skip.
          if (previouslyKnownLeader == null) {
            // We have no clue as to who the previous leader was, hence the host information will
            // be empty.
            LOGGER.error(String
                .format("LEADER_DOWN (Published) resource:%s, partition=%s, leader:unknown",
                    resourceName, partitionName));
            resourceLeaderCache.put(partitionName, new Leader(null, LeaderState.LEADER_DOWN));
            publishStatus(resourceName, partitionName, null, LeaderState.LEADER_DOWN);
          } else {
            if (previouslyKnownLeader.getState() == LeaderState.LEADER_UP) {
              // Previously known leader was up, and now it is down. Hence publish the event.
              String prevLeaderHost = previouslyKnownLeader.getLeaderInstanceId();
              LOGGER.error(String
                  .format("LEADER_DOWN (Published) resource:%s, partition=%s, leader:%s",
                      resourceName, partitionName, prevLeaderHost));
              resourceLeaderCache.put(partitionName,
                  new Leader(prevLeaderHost, LeaderState.LEADER_DOWN));
              publishStatus(resourceName, partitionName, prevLeaderHost, LeaderState.LEADER_DOWN);
            } else if (previouslyKnownLeader.getState() == LeaderState.LEADER_DOWN) {
              // Duplicate event of downed instance; whether we know or not which host it was,
              // no point in publishing this event again.
              LOGGER.error(String
                  .format(
                      "LEADER_DOWN (Duplicate/Skipped) resource:%s, partition=%s, leader:unknown",
                      resourceName, partitionName));
              doNothing();
            }
          }
        }
      }
    }
  }
}
