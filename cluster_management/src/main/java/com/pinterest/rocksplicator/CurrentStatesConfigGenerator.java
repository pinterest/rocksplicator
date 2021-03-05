package com.pinterest.rocksplicator;

import com.pinterest.rocksplicator.eventstore.ExternalViewLeaderEventLogger;
import com.pinterest.rocksplicator.monitoring.mbeans.RocksplicatorMonitor;
import com.pinterest.rocksplicator.publisher.ShardMapPublisher;
import com.pinterest.rocksplicator.utils.AutoCloseableLock;
import com.pinterest.rocksplicator.utils.ExternalViewUtils;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.api.listeners.ExternalViewChangeListener;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
public class CurrentStatesConfigGenerator
    implements RoutingTableChangeListener,
               ExternalViewChangeListener,
               IdealStateChangeListener,
               ConfigGeneratorIface {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigGenerator.class);
  private final String clusterName;
  private final Map<String, String> hostToHostWithDomain;
  private final HelixManager helixManager;
  private final ReentrantLock synchronizedCallbackLock;
  private final ShardMapPublisher shardMapPublisher;
  private final RocksplicatorMonitor monitor;
  private ExternalViewLeaderEventLogger externalViewLeaderEventLogger;

  private ConcurrentMap<String, IdealState> idealStatesByResourceName;
  private ConcurrentMap<String, ExternalView> externalViewsByResourceName;
  private RoutingTableProvider routingTableProvider;

  public CurrentStatesConfigGenerator(
      final String clusterName,
      final HelixManager helixManager,
      final ShardMapPublisher<JSONObject> shardMapPublisher,
      final RocksplicatorMonitor monitor,
      final ExternalViewLeaderEventLogger externalViewLeaderEventLogger) throws Exception {
    this.clusterName = clusterName;
    this.helixManager = helixManager;
    this.shardMapPublisher = shardMapPublisher;
    this.hostToHostWithDomain = new HashMap<String, String>();
    this.monitor = monitor;
    this.synchronizedCallbackLock = new ReentrantLock();
    this.externalViewLeaderEventLogger = externalViewLeaderEventLogger;

    /**
     * Ensure these are added before routing table is created. Only when  we have idealStates
     * and externalViews for any resource, that we create the routing table based on current States.
     */
    helixManager.addIdealStateChangeListener(this);
    helixManager.addExternalViewChangeListener(this);

  }

  private void initializeRoutingTable() {
    try (AutoCloseableLock autoCloseableLock =
             new AutoCloseableLock(this.synchronizedCallbackLock)) {
      /**
       * Initialize the routing table provider, once
       */
      if (idealStatesByResourceName != null      // Ensure idealStates has been initialized.
          && externalViewsByResourceName != null // Ensure externalViews has also been initialized.
          && routingTableProvider == null // Ensure that routingTable has not yet been initialized.
      ) {
        this.routingTableProvider =
            new RoutingTableProvider(this.helixManager, PropertyType.CURRENTSTATES);
        this.routingTableProvider.addRoutingTableChangeListener(this, this);
      }
    }
  }

  private void logUncheckedException(Runnable r) {
    try {
      r.run();
    } catch (Throwable throwable) {
      this.monitor.incrementConfigGeneratorFailCount();
      LOG.error(String.format("cluster:%s Exception in generateShardConfig()", clusterName),
          throwable);
      throw throwable;
    }
  }

  @Override
  public void onIdealStateChange(List<IdealState> idealStates, NotificationContext changeContext)
      throws InterruptedException {

    try (AutoCloseableLock autoLock = AutoCloseableLock.lock(this.synchronizedCallbackLock)) {
      logUncheckedException(new Runnable() {
        @Override
        public void run() {
          for (IdealState idealState : idealStates) {
            String resourceName = idealState.getResourceName();
            if (resourceName == null
                || resourceName.startsWith("PARTICIPANT_LEADER")
                || isTaskResource(idealState)) {
              continue;
            }

            /**
             * First time initialization of idealStatesByResourceName
             */
            if (idealStatesByResourceName == null) {
              idealStatesByResourceName = new ConcurrentHashMap<>();
            }

            idealStatesByResourceName.put(resourceName, idealState);

            LOG.error(String
                .format(
                    "Got IdealState for cluster: %s, resource: %s, resource_type:%s idealState: %s",
                    clusterName,
                    idealState.getResourceName(),
                    idealState.getResourceType(),
                    idealState));
          }
          if (routingTableProvider == null) {
            initializeRoutingTable();
          }
        }
      });
    }
    updateAfterExternalNotificfations();
  }


  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
                                   NotificationContext changeContext) {
    try (AutoCloseableLock autoLock = AutoCloseableLock.lock(this.synchronizedCallbackLock)) {
      logUncheckedException(new Runnable() {
        @Override
        public void run() {
          for (ExternalView externalView : externalViewList) {
            String resourceName = externalView.getResourceName();
            if (resourceName == null
                || resourceName.startsWith("PARTICIPANT_LEADER")
                || isTaskResource(externalView)) {
              continue;
            }

            /**
             * First time initialization of externalViewsByResourceName
             */
            if (externalViewsByResourceName == null) {
              externalViewsByResourceName = new ConcurrentHashMap<>();
            }

            externalViewsByResourceName.put(resourceName, externalView);

            LOG.error(String
                .format("Got ExternalView for cluster: %s, resource: %s, externalView: %s",
                    clusterName,
                    externalView.getResourceName(),
                    externalView));
          }

          if (routingTableProvider == null) {
            initializeRoutingTable();
          }
        }
      });
    }
    updateAfterExternalNotificfations();
  }

  private void updateAfterExternalNotificfations() {
    try (AutoCloseableLock autoLock = AutoCloseableLock.lock(this.synchronizedCallbackLock)) {
      logUncheckedException(new Runnable() {
        @Override
        public void run() {
          if (routingTableProvider != null) {
            uncheckedGenerateConfig(routingTableProvider.getRoutingTableSnapshot());
          }
        }
      });
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

  /***
   * Lock idealState changes here...
   */
  private void uncheckedGenerateConfig(RoutingTableSnapshot routingTableSnapshot) {
    final long generationStartTimeMillis = System.currentTimeMillis();

    this.monitor.incrementConfigGeneratorCalledCount();
    Stopwatch stopwatch = Stopwatch.createStarted();

    // Make sure only resources available in the cluster are counted.
    Collection<String> availableResources = routingTableSnapshot.getResources();

    // Prune idealStates / externalViews depending based on existing resources.
    idealStatesByResourceName.keySet().retainAll(availableResources);
    externalViewsByResourceName.keySet().retainAll(availableResources);

    Collection<InstanceConfig> instanceConfigs = routingTableSnapshot.getInstanceConfigs();
    Collection<LiveInstance> liveInstances = routingTableSnapshot.getLiveInstances();

    final Map<String, LiveInstance> liveInstancesMap = liveInstances.stream()
        .filter(liveInstance -> liveInstance != null)
        .filter(liveInstance -> liveInstance.isValid())
        .collect(Collectors.toMap(liveInstance -> liveInstance.getInstanceName(), v -> v));

    final Set<String> validResources = availableResources.stream()
        .filter(resource -> resource != null)
        .filter(resource -> !resource.startsWith("PARTICIPANT_LEADER"))
        .filter(resource -> idealStatesByResourceName.containsKey(resource))
        .collect(Collectors.toSet());

    final Map<String, InstanceConfig> instanceConfigMap = instanceConfigs.stream()
        .filter(instanceConfig -> instanceConfig != null)
        .filter(instanceConfig -> instanceConfig.getInstanceName() != null)
        .filter(instanceConfig -> instanceConfig.getInstanceEnabled())
        .filter(instanceConfig -> !instanceConfig.containsTag("disabled"))
        .filter(instanceConfig -> liveInstancesMap.containsKey(instanceConfig.getInstanceName()))
        .filter(instanceConfig -> liveInstancesMap.get(instanceConfig.getInstanceName()) != null)
        .collect(Collectors.toMap(instanceConfig -> instanceConfig.getInstanceName(), v -> v));

    List<ExternalView> externalViewsToProcess =
        Lists.newArrayListWithCapacity(idealStatesByResourceName.size());

    Set<String> existingHosts = new HashSet<>();
    JSONObject jsonClusterShardMap = new JSONObject();

    Set<String> disabledHosts = new HashSet<>();

    for (String resource : validResources) {
      IdealState idealState = idealStatesByResourceName.get(resource);
      if (idealState == null) {
        continue;
      }
      if (isTaskResource(idealState)) {
        continue;
      }

      ExternalView externalView = createExternalView(
          idealStatesByResourceName.get(resource),
          externalViewsByResourceName.get(resource),
          routingTableSnapshot);

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
        externalViewsToProcess.stream().map(e -> e.getResourceName()).collect(Collectors.toSet()),
        externalViewsToProcess,
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

  private ExternalView createExternalView(
      final IdealState idealState,
      final ExternalView previousExternalView,
      final RoutingTableSnapshot snapshot) {
    // First number of partitions
    int numPartitions = idealState.getNumPartitions();
    String stateModel = idealState.getStateModelDefRef();
    List<String> states = ImmutableList.of();
    if (stateModel.equals("LeaderFollower")) {
      states = ImmutableList.of("LEADER", "FOLLOWER");
    } else if (stateModel.equals("MasterSlave")) {
      states = ImmutableList.of("MASTER", "SLAVE");
    } else if (stateModel.equals("OnlineOffline")) {
      states = ImmutableList.of("ONLINE");
    }

    ExternalView externalView = new ExternalView(idealState.getResourceName());
    if (previousExternalView == null || previousExternalView.getStat() == null) {
      externalView.setStat(idealState.getStat());
    } else {
      HelixProperty.Stat prevStat = previousExternalView.getStat();
      HelixProperty.Stat currentStat = new HelixProperty.Stat();
      currentStat.setCreationTime(prevStat.getCreationTime());
      currentStat.setVersion(prevStat.getVersion());
      currentStat.setEphemeralOwner(prevStat.getEphemeralOwner());
      currentStat.setModifiedTime(System.currentTimeMillis());
      externalView.setStat(currentStat);
    }
    externalView.setBucketSize(idealState.getBucketSize());
    externalView.setBatchMessageMode(idealState.getBatchMessageMode());
    for (int partId = 0; partId < numPartitions; ++partId) {
      String helixPartitionName = String.format("%s_%s", idealState.getResourceName(), partId);
      for (String state : states) {
        List<InstanceConfig> instanceConfigs = snapshot.getInstancesForResource(
            idealState.getResourceName(), helixPartitionName, state);
        for (InstanceConfig instanceConfig : instanceConfigs) {
          externalView.setState(helixPartitionName, instanceConfig.getInstanceName(), state);
        }
      }
    }
    externalView.getRecord().setSimpleField("NUM_PARTITIONS", Integer.toString(numPartitions));
    return externalView;
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
   * only keep db resources from external view
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

  /**
   * filter out resources with "Task" state model (ie. workflows and jobs);
   * only keep db resources from ideal states
   */
  private boolean isTaskResource(IdealState idealState) {
    if (idealState != null) {
      String stateMode = idealState.getStateModelDefRef();
      if (stateMode != null && stateMode.equals("Task")) {
        return true;
      }
    } else {
      LOG.error(
          "Did not remove resource from shard map generation, due to can't get ideal state for "
              + idealState.getResourceName());
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

      PropertyKey.Builder keyBuilder = helixManager.getHelixDataAccessor().keyBuilder();
      helixManager.removeListener(keyBuilder.externalViews(), this);
      helixManager.removeListener(keyBuilder.idealStates(), this);

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
