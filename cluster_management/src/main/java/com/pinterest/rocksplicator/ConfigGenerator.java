/// Copyright 2017 Pinterest Inc.
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
// @author bol (bol@pinterest.com)
//

package com.pinterest.rocksplicator;

import com.pinterest.rocksplicator.eventstore.ExternalViewLeaderEventLogger;
import com.pinterest.rocksplicator.monitoring.mbeans.RocksplicatorMonitor;
import com.pinterest.rocksplicator.publisher.ShardMapPublisher;
import com.pinterest.rocksplicator.utils.AutoCloseableLock;
import com.pinterest.rocksplicator.utils.ExternalViewUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.CustomCodeCallbackHandler;
import org.apache.helix.spectator.RoutingTableProvider;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**

 The Config Generator current produces the shard_map config in json format and posts it to
 provided url. The top level json object contains jsonobjects per resource.
 For each resource, we have one entry num_shards specifying the number of partitions
 available in the resource.

 Within given resource, we aggregrate at per instance level, all the partitions that are in
 serving state.

 The instance on which any partition of a given resource is hosted, is specified in following
 format.
 ${ip_address}:${thrift_service_port}:${az}_${pg}

 The thrift service port is the port where the applications api endpoints are available
 for thrift rpc calls.

 The partition is specified as a partition number between 0 and num_shards. The partition numbers
 are padded with leading zero, and is of fixed length of size 5.
 The format is as follows
 [0000]${partition_id}[:[M,S]]. The partition is specified with M or S state specifier in case of
 LeaderFollower / MasterSlave model. In case of OnlineOffline / Bootstrap statemodels, only
 serving partitions are Online state. Hence in that case the state specified is not appended to the
 partition id.

 <code>

 Following is the shard_map config for MasterSlave / LeaderFollower statemodel.
 {
 "resource_name": {
 "instance1:port:us-east-1d_us-east-1d": [
 "00000:S",
 "00001:S",       <-- :S signifies that it is a follower/slave
 "00002:M",       <-- :M signifies that it is a leader/master
 "00003:S",       <-- the partition number are padded with leading zero, and is fixed length
 "00004:S",
 "00005:M",
 "00006:M",
 "00007:S",
 "00008:S"
 ],
 "instance2:port:us-east-1e_us-east-1e": [
 "00000:S",
 "00001:M",
 "00002:S",
 "00003:M",
 "00004:M",
 "00005:S",
 "00006:S",
 "00007:S",
 "00008:S"
 ],
 "instance3:port:us-east-1a_us-east-1a": [
 "00000:M",
 "00001:S",
 "00002:S",
 "00003:S",
 "00004:S",
 "00005:S",
 "00006:S",
 "00007:M",
 "00008:M"
 ],
 "num_shards": 9
 }
 }

 In case of OnlineOffline / Bootstrap model, the :M, :S specifier is not available.

 {
 "resource_name": {
 "instance1:port:us-east-1d_us-east-1d": [
 "00000",
 "00001",
 "00002",
 "00003",
 "00004",
 "00005",
 "00006",
 "00007",
 "00008"
 ],
 "instance2:port:us-east-1e_us-east-1e": [
 "00000",
 "00001",
 "00002",
 "00003",
 "00004",
 "00005",
 "00006",
 "00007",
 "00008"
 ],
 "instance3:port:us-east-1a_us-east-1a": [
 "00000",
 "00001",
 "00002",
 "00003",
 "00004",
 "00005",
 "00006",
 "00007",
 "00008"
 ],
 "num_shards": 9
 }
 }
 </code>


 */
public class ConfigGenerator extends RoutingTableProvider implements CustomCodeCallbackHandler,
                                                                     Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigGenerator.class);

  private final String clusterName;
  private final Map<String, String> hostToHostWithDomain;
  private final RocksplicatorMonitor monitor;
  private final ReentrantLock synchronizedCallbackLock;
  private final ShardMapPublisher shardMapPublisher;
  private HelixManager helixManager;
  private HelixAdmin helixAdmin;
  private Set<String> disabledHosts;
  private ExternalViewLeaderEventLogger externalViewLeaderEventLogger;

  @VisibleForTesting
  ConfigGenerator(
      final String clusterName,
      final HelixManager helixManager,
      final ShardMapPublisher<JSONObject> shardMapPublisher) {
    this(clusterName, helixManager, shardMapPublisher,
        new RocksplicatorMonitor(clusterName, helixManager.getInstanceName()), null);
  }

  public ConfigGenerator(
      final String clusterName,
      final HelixManager helixManager,
      final ShardMapPublisher<JSONObject> shardMapPublisher,
      final RocksplicatorMonitor monitor,
      final ExternalViewLeaderEventLogger externalViewLeaderEventLogger) {
    this.clusterName = clusterName;
    this.helixManager = helixManager;
    this.helixAdmin = this.helixManager.getClusterManagmentTool();
    this.hostToHostWithDomain = new HashMap<String, String>();
    this.shardMapPublisher = shardMapPublisher;
    this.disabledHosts = new HashSet<>();
    this.monitor = monitor;
    this.synchronizedCallbackLock = new ReentrantLock();
    this.externalViewLeaderEventLogger = externalViewLeaderEventLogger;
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

  /**
   * We are not 100% confident on behaviour of helix agent w.r.t. threading and execution model
   * for callback functions call from helix agent. Especially is is possible to get multiple
   * callbacks, of same of different types, sources to be called by helix in parallel.
   *
   * In order to ensure that only one callback is actively being processed at any time, we
   * explicitly guard any callback function body with a single re-entrant lock. This provides
   * explicit guarantee around the behaviour of how shard_maps are processed, generated and
   * published.
   */
  @Override
  public void onCallback(final NotificationContext notificationContext) {
    try (AutoCloseableLock autoLock = AutoCloseableLock.lock(this.synchronizedCallbackLock)) {
      logUncheckedException(new Runnable() {
        @Override
        public void run() {
          LOG.error("Received notification: " + notificationContext.getChangeType());
          if (notificationContext.getChangeType() == HelixConstants.ChangeType.EXTERNAL_VIEW) {
            generateShardConfig();
          } else if (notificationContext.getChangeType() == HelixConstants.ChangeType.INSTANCE_CONFIG) {
            if (updateDisabledHosts()) {
              generateShardConfig();
            }
          }
        }
      });
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void onConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
    try (AutoCloseableLock autoLock = AutoCloseableLock.lock(this.synchronizedCallbackLock)) {
      logUncheckedException(new Runnable() {
        @Override
        public void run() {
          if (updateDisabledHosts()) {
            generateShardConfig();
          }
        }
      });
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void onExternalViewChange(List<ExternalView> externalViewList,
                                   NotificationContext changeContext) {
    try (AutoCloseableLock autoLock = AutoCloseableLock.lock(this.synchronizedCallbackLock)) {
      logUncheckedException(new Runnable() {
        @Override
        public void run() {
          generateShardConfig();
        }
      });
    }
  }

  private void generateShardConfig() {
    final long generationStartTimeMillis = System.currentTimeMillis();

    this.monitor.incrementConfigGeneratorCalledCount();
    Stopwatch stopwatch = Stopwatch.createStarted();

    List<String> resources = this.helixAdmin.getResourcesInCluster(clusterName);
    filterOutTaskResources(resources);

    // Resources starting with PARTICIPANT_LEADER is for HelixCustomCodeRunner
    resources = resources.stream().filter(r -> ! r.startsWith("PARTICIPANT_LEADER")).collect(Collectors.toList());

    Set<String> existingHosts = new HashSet<String>();

    List<ExternalView> externalViewsToProcess = null;
    if (externalViewLeaderEventLogger != null) {
      externalViewsToProcess = new ArrayList<>();
    }

    // compose cluster config
    JSONObject jsonClusterShardMap = new JSONObject();
    for (String resource : resources) {
      ExternalView externalView = this.helixAdmin.getResourceExternalView(clusterName, resource);
      if (externalView == null) {
        this.monitor.incrementConfigGeneratorNullExternalView();
        LOG.error("Failed to get externalView for resource: " + resource);
        /**
         * In some situations, we may encounter a null externalView for a given resource.
         * This can happen, since there exists a race condition between retrieving list of resources
         * and then iterating over each of those resources to retrieve corresponding externalView
         * and potential deletion of a resource in between.
         *
         * Another situation where we may receive null externalView for a resource is in case where
         * a resource is newly created but it's externalView has not yet been generated by helix
         * controller.
         *
         * There can be quite a few race conditions which are difficult to enumerate and under such
         * scenarios, we can't guarantee that externalView for a given resource exists at a specific
         * moment. In such cases, it is safe to ignore such resource until it's externalView is
         * accessible and not null.
         */
        continue;
      }

      if (externalViewLeaderEventLogger != null && externalViewsToProcess != null) {
        externalViewsToProcess.add(externalView);
      }

      Set<String> partitions = externalView.getPartitionSet();

      // compose resource config
      JSONObject resourceConfig = new JSONObject();
      String partitionsStr = externalView.getRecord().getSimpleField("NUM_PARTITIONS");
      resourceConfig.put("num_shards", Integer.parseInt(partitionsStr));

      // build host to partition list map
      Map<String, List<String>> hostToPartitionList = new HashMap<String, List<String>>();
      for (String partition : partitions) {
        String[] parts = partition.split("_");
        String partitionNumber =
            String.format("%05d", Integer.parseInt(parts[parts.length - 1]));
        Map<String, String> hostToState = externalView.getStateMap(partition);
        for (Map.Entry<String, String> entry : hostToState.entrySet()) {
          existingHosts.add(entry.getKey());

          /**TODO: gopalrajpurohit
           * Add a LiveInstanceListener and remove any temporary / permanently dead hosts
           * from consideration. This is to ensure that during deploys, we take into account
           * downed instances faster then potentially available through externalViews.
           */
          if (disabledHosts.contains(entry.getKey())) {
            // exclude disabled hosts from the shard map config
            continue;
          }

          String state = entry.getValue();
          if (!ExternalViewUtils.isServing(state)) {
            continue;
          }

          String hostWithDomain = getHostWithDomain(entry.getKey());
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

    // remove host that doesn't exist in the ExternalView from hostToHostWithDomain
    hostToHostWithDomain.keySet().retainAll(existingHosts);

    /**
     * Finally publish the shard_map in json_format to multiple configured publishers.
     */
    shardMapPublisher.publish(
        resources.stream().collect(Collectors.toSet()), externalViewsToProcess, jsonClusterShardMap);

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

  private String getHostWithDomain(String host) {
    String hostWithDomain = hostToHostWithDomain.get(host);
    if (hostWithDomain != null) {
      return hostWithDomain;
    }

    // local cache missed, read from ZK
    InstanceConfig instanceConfig = this.helixAdmin.getInstanceConfig(clusterName, host);
    String domain = instanceConfig.getDomain();
    String[] parts = domain.split(",");
    String az = parts[0].split("=")[1];
    String pg = parts[1].split("=")[1];
    hostWithDomain = host.replace('_', ':') + ":" + az + "_" + pg;
    hostToHostWithDomain.put(host, hostWithDomain);
    return hostWithDomain;
  }

  // update disabledHosts, return true if there is any changes
  private boolean updateDisabledHosts() {
    Set<String> latestDisabledInstances = new HashSet<>(
        this.helixAdmin.getInstancesInClusterWithTag(clusterName, "disabled"));

    if (disabledHosts.equals(latestDisabledInstances)) {
      // no changes
      LOG.error("No changes to disabled instances");
      return false;
    }
    disabledHosts = latestDisabledInstances;
    return true;
  }

  /**
   * filter out resources with "Task" state model (ie. workflows and jobs);
   * only keep db resources from ideal states
   */
  public void filterOutTaskResources(List<String> resources) {
    Iterator<String> iter = resources.iterator();
    while (iter.hasNext()) {
      String res = iter.next();
      IdealState ideal = this.helixAdmin.getResourceIdealState(clusterName, res);
      if (ideal != null) {
        String stateMode = ideal.getStateModelDefRef();
        if (stateMode != null && stateMode.equals("Task")) {
          iter.remove();
        }
      } else {
        LOG.error(
            "Did not remove resource from shard map generation, due to can't get ideal state for "
                + res);
      }
    }
  }

  @Override
  public void close() throws IOException {
    // ensure the while we are closing the ConfigGenerator, we are not processing any callback
    // at the moment.
    try (AutoCloseableLock lock = new AutoCloseableLock(this.synchronizedCallbackLock)) {
      // Cleanup any remaining items.
      externalViewLeaderEventLogger.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
