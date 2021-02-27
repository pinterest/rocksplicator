package com.pinterest.rocksplicator;

import com.pinterest.rocksplicator.eventstore.ExternalViewLeaderEventLogger;
import com.pinterest.rocksplicator.monitoring.mbeans.RocksplicatorMonitor;
import com.pinterest.rocksplicator.publisher.ShardMapPublisher;
import com.pinterest.rocksplicator.utils.AutoCloseableLock;
import com.pinterest.rocksplicator.utils.ExternalViewUtils;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyType;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * CurrentStates ConfigGenerator using CurrentStates based RoutingTable.
 *
 * Note: This class cann't be used in a Spectator embedded in Participant that uses
 * HelixCustomCodeRunner class. This is because in case of Spectator embedded in Participant,
 * we want to ensure that only one ConfigGenerator is running. In case of old ConfigGenerator
 * the config is generated based on notification provided by HelixCustomCodeRunner class, if
 * the instance Participant is a leader. However, to use this class and guarantee that only
 * one instance is active throughout Participant cluster, we need a notification on when the
 * leadership is acquired and when is it released. This is because we manage the notification
 * from helix directly through use of RoutingTableProvider. Hence even if helixManager is shared
 * between the object of this class and the HelixCustomCodeRunner, the listeners of
 * RoutingTableProvider will not be unregistered when the leadership is lost.
 *
 * Hence is is recommended that this class be only used in either Spectator as a Stand Alone mode
 * or the DistributedSpectator which provides hooks for leadership lost v/s leadership acquired.
 */
public class CurrentStatesConfigGenerator implements RoutingTableChangeListener, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigGenerator.class);
  private final String clusterName;
  private final Map<String, String> hostToHostWithDomain;
  private final HelixManager helixManager;
  private final ReentrantLock synchronizedCallbackLock;
  private final ShardMapPublisher shardMapPublisher;
  private final RocksplicatorMonitor monitor;
  private ExternalViewLeaderEventLogger externalViewLeaderEventLogger;
  private RoutingTableProvider routingTableProvider;

  public CurrentStatesConfigGenerator(
      final String clusterName,
      final HelixManager helixManager,
      final ShardMapPublisher<JSONObject> shardMapPublisher,
      final RocksplicatorMonitor monitor,
      final ExternalViewLeaderEventLogger externalViewLeaderEventLogger) {
    this.clusterName = clusterName;
    this.helixManager = helixManager;
    this.shardMapPublisher = shardMapPublisher;
    this.hostToHostWithDomain = new HashMap<String, String>();
    this.monitor = monitor;
    this.synchronizedCallbackLock = new ReentrantLock();
    this.externalViewLeaderEventLogger = externalViewLeaderEventLogger;

    this.routingTableProvider = new RoutingTableProvider(this.helixManager, PropertyType.CURRENTSTATES);
    this.routingTableProvider.addRoutingTableChangeListener(this, this);
  }

  private void logUncheckedException(Runnable r) {
    try {
      r.run();
    } catch (Throwable throwable) {
      this.monitor.incrementConfigGeneratorFailCount();
      LOG.error("Exception in generateShardConfig()", throwable);
      throw throwable;
    }
  }

  @Override
  public void onRoutingTableChange(
      final RoutingTableSnapshot routingTableSnapshot,
      final Object context) {
    // Generate externalViews;
    try (AutoCloseableLock autoLock = AutoCloseableLock.lock(this.synchronizedCallbackLock)) {
      logUncheckedException(new Runnable() {
        @Override
        public void run() {
          uncheckedGenerateConfig(routingTableSnapshot);
        }
      });
    }
  }

  private void uncheckedGenerateConfig(RoutingTableSnapshot routingTableSnapshot) {
    final long generationStartTimeMillis = System.currentTimeMillis();

    this.monitor.incrementConfigGeneratorCalledCount();
    Stopwatch stopwatch = Stopwatch.createStarted();

    Collection<ExternalView> externalViews = routingTableSnapshot.getExternalViews();
    Collection<InstanceConfig> instanceConfigs = routingTableSnapshot.getInstanceConfigs();
    Collection<LiveInstance> liveInstances = routingTableSnapshot.getLiveInstances();
    Collection<String> resources = routingTableSnapshot.getResources();

    final Map<String, ExternalView> externalViewMap = externalViews.stream()
        .filter(externalView -> externalView != null)
        .filter(externalView -> externalView.getResourceName() != null)
        .filter(externalView -> !isTaskResource(externalView))
        .collect(Collectors.toMap(e -> e.getResourceName(), v -> v));

    final Map<String, LiveInstance> liveInstancesMap = liveInstances.stream()
        .filter(liveInstance -> liveInstance != null)
        .filter(liveInstance -> liveInstance.isValid())
        .collect(Collectors.toMap(liveInstance -> liveInstance.getInstanceName(), v -> v));

    final Set<String> validResources = resources.stream()
        .filter(resource -> resource != null)
        .filter(resource -> !resource.startsWith("PARTICIPANT_LEADER"))
        .filter(resource -> externalViewMap.containsKey(resource))
        .collect(Collectors.toSet());

    final Map<String, InstanceConfig> instanceConfigMap = instanceConfigs.stream()
        .filter(instanceConfig -> instanceConfig != null)
        .filter(instanceConfig -> instanceConfig.getInstanceName() != null)
        .filter(instanceConfig -> instanceConfig.getInstanceEnabled())
        .filter(instanceConfig -> !instanceConfig.containsTag("disabled"))
        .filter(instanceConfig -> liveInstancesMap.containsKey(instanceConfig.getInstanceName()))
        .filter(instanceConfig -> liveInstancesMap.get(instanceConfig.getInstanceName()) != null)
        .collect(Collectors.toMap(instanceConfig -> instanceConfig.getInstanceName(), v -> v));

    List<ExternalView>
        externalViewsToProcess =
        Lists.newArrayListWithCapacity(externalViewMap.size());
    Set<String> existingHosts = new HashSet<>();
    JSONObject jsonClusterShardMap = new JSONObject();

    Set<String> disabledHosts = new HashSet<>();

    for (String resource : validResources) {
      ExternalView externalView = externalViewMap.get(resource);

      // compose resource config
      JSONObject resourceConfig = new JSONObject();
      String partitionsStr = externalView.getRecord().getSimpleField("NUM_PARTITIONS");
      resourceConfig.put("num_shards", Integer.parseInt(partitionsStr));

      externalViewsToProcess.add(externalView);

      Set<String> partitions = externalView.getPartitionSet();
      // build host to partition list map
      Map<String, List<String>> hostToPartitionList = new HashMap<String, List<String>>();
      for (String partition : partitions) {
        String[] parts = partition.split("_");
        String partitionNumber = String.format("%05d", Integer.parseInt(parts[parts.length - 1]));
        Map<String, String> hostToState = externalView.getStateMap(partition);
        for (Map.Entry<String, String> entry : hostToState.entrySet()) {
          String helixHostName = entry.getKey();
          InstanceConfig instanceConfig = instanceConfigMap.get(helixHostName);
          LiveInstance liveInstance = liveInstancesMap.get(helixHostName);

          if (instanceConfig == null || liveInstance == null) {
            disabledHosts.add(helixHostName);
            continue;
          }

          if (!instanceConfig.getInstanceEnabled()
              || instanceConfig.containsTag("disabled")
              || !liveInstance.isValid()) {
            disabledHosts.add(helixHostName);
            continue;
          }

          existingHosts.add(helixHostName);

          String state = entry.getValue();
          if (!ExternalViewUtils.isServing(state)) {
            continue;
          }

          String hostWithDomain = getHostWithDomain(helixHostName, instanceConfig);
          List<String> partitionList = hostToPartitionList.get(hostWithDomain);
          if (partitionList == null) {
            partitionList = new ArrayList<String>();
            hostToPartitionList.put(hostWithDomain, partitionList);
          }
          partitionList.add(partitionNumber + ExternalViewUtils.getShortHandState(state));
        }
      }

      // Add host to partition list map to the resource config
      for (Map.Entry<String, List<String>> entry : hostToPartitionList.entrySet()) {
        JSONArray jsonArray = new JSONArray();
        for (String p : entry.getValue()) {
          jsonArray.add(p);
        }

        resourceConfig.put(entry.getKey(), jsonArray);
      }

      // add the resource config to the cluster config
      jsonClusterShardMap.put(resource, resourceConfig);
    }

    hostToHostWithDomain.keySet().retainAll(existingHosts);

    /**
     * Finally publish the shard_map in json_format to multiple configured publishers.
     */
    shardMapPublisher.publish(
        validResources.stream().collect(Collectors.toSet()),
        Lists.newArrayList(externalViewMap.values()),
        jsonClusterShardMap);

    long shardPostingTimeMillis = System.currentTimeMillis();

    stopwatch.stop();
    long elapsedMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    this.monitor.reportConfigGeneratorLatency(elapsedMs);

    // All side-effects must be done, after the shard-map is posted...
    if (externalViewLeaderEventLogger != null) {
      LOG.error("Processing ExternalViews for LeaderEventsLogger");
      externalViewLeaderEventLogger.process(
          externalViewsToProcess, disabledHosts, generationStartTimeMillis, shardPostingTimeMillis);
    }
  }

  private String getHostWithDomain(String host, InstanceConfig instanceConfig) {
    String hostWithDomain = hostToHostWithDomain.get(host);
    if (hostWithDomain != null) {
      return hostWithDomain;
    }

    // local cache missed, read from ZK
    String domain = instanceConfig.getDomain();
    String[] parts = domain.split(",");
    String az = parts[0].split("=")[1];
    String pg = parts[1].split("=")[1];
    hostWithDomain = host.replace('_', ':') + ":" + az + "_" + pg;
    hostToHostWithDomain.put(host, hostWithDomain);
    return hostWithDomain;
  }

  /**
   * filter out resources with "Task" state model (ie. workflows and jobs);
   * only keep db resources from ideal states
   */
  private boolean isTaskResource(ExternalView externalView) {
    if (externalView != null) {
      String stateMode = externalView.getStateModelDefRef();
      if (stateMode != null && stateMode.equals("Task")) {
        return true;
      }
    } else {
      LOG.error(
          "Did not remove resource from shard map generation, due to can't get ideal state for "
              + externalView.getResourceName());
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    // ensure the while we are closing the CurrentStatesConfigGenerator,
    // we are not processing any callback at the moment.
    try (AutoCloseableLock lock = new AutoCloseableLock(this.synchronizedCallbackLock)) {
      // Cleanup any remaining items.
      if (this.routingTableProvider != null) {
        try {
          this.routingTableProvider.shutdown();
        } catch (Exception e) {
          LOG.error("Error closing RoutingTableProvider");
        }
        this.routingTableProvider = null;
      }

      try {
        shardMapPublisher.close();
      } catch (IOException io) {
        LOG.error("Error closing shardMapPublisher: ", io);
      }

      if (externalViewLeaderEventLogger != null) {
        try {
          externalViewLeaderEventLogger.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
