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

import com.pinterest.rocksplicator.monitoring.mbeans.RocksplicatorMonitor;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.participant.CustomCodeCallbackHandler;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
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

public class ConfigGenerator extends RoutingTableProvider implements CustomCodeCallbackHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigGenerator.class);
  public static final HelixConstants.ChangeType[] ALLOWED_CHANGE_TYPES_ARRAY
      = new HelixConstants.ChangeType[]{
          HelixConstants.ChangeType.EXTERNAL_VIEW,
          HelixConstants.ChangeType.CONFIG,
          HelixConstants.ChangeType.LIVE_INSTANCE};

  private static final Set<HelixConstants.ChangeType> allowedChangeTypes =
      ImmutableSet.copyOf(ALLOWED_CHANGE_TYPES_ARRAY);

  private final String clusterName;
  private HelixManager helixManager;
  private HelixAdmin helixAdmin;
  private Map<String, String> hostToHostWithDomain;
  private final String postUrl;
  private JSONObject dataParameters;
  private String lastPostedContent;
  private Set<String> disabledHosts;
  private RocksplicatorMonitor monitor;
  private final boolean enableDumpToLocal;
  private final ReentrantLock updateLock;

  public ConfigGenerator(String clusterName, HelixManager helixManager, String configPostUrl) {
    this(clusterName, helixManager, configPostUrl,
        new RocksplicatorMonitor(clusterName, helixManager.getInstanceName()));
  }

  public ConfigGenerator(String clusterName, HelixManager helixManager, String configPostUrl,
                         RocksplicatorMonitor monitor) {
    this.clusterName = clusterName;
    this.helixManager = helixManager;
    this.helixAdmin = helixManager.getClusterManagmentTool();
    this.hostToHostWithDomain = new HashMap<String, String>();
    this.postUrl = configPostUrl;
    this.dataParameters = new JSONObject();
    this.dataParameters.put("config_version", "v3");
    this.dataParameters.put("author", "ConfigGenerator");
    this.dataParameters.put("comment", "new shard config");
    this.dataParameters.put("content", "{}");
    this.lastPostedContent = null;
    this.disabledHosts = new HashSet<>();
    this.monitor = monitor;
    this.enableDumpToLocal = new File("/var/log/helixspectator").canWrite();
    this.updateLock = new ReentrantLock();
  }

  @Override
  public void onCallback(NotificationContext notificationContext) {
    LOG.error("Received notification: " + notificationContext.getChangeType());

    if (allowedChangeTypes.contains(notificationContext.getChangeType())) {
      generateShardConfigWithUpdatedDisabledHosts();
    }
  }

  @Override
  @PreFetch(enabled = false)
  public void onExternalViewChange(List<ExternalView> externalViewList, NotificationContext changeContext) {
    super.onExternalViewChange(externalViewList, changeContext);
    generateShardConfigWithoutUpdatedDisabledHosts();
  }

  @Override
  @PreFetch(enabled = false)
  public void onConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
    super.onConfigChange(configs, changeContext);
    generateShardConfigWithUpdatedDisabledHosts();
  }

  @Override
  @PreFetch(enabled = false)
  public void onInstanceConfigChange(List<InstanceConfig> configs,
                                     NotificationContext changeContext) {
    super.onInstanceConfigChange(configs, changeContext);
    generateShardConfigWithUpdatedDisabledHosts();
  }

  @Override
  @PreFetch(enabled = false)
  public void onLiveInstanceChange(List<LiveInstance> liveInstances,
                                   NotificationContext changeContext) {
    super.onLiveInstanceChange(liveInstances, changeContext);
    generateShardConfigWithUpdatedDisabledHosts();
  }

  private void generateShardConfigWithoutUpdatedDisabledHosts() {
    updateLock.lock();
    try {
      generateShardConfigThreadUnsafe();
    } finally {
      updateLock.unlock();
    }
  }

  private void generateShardConfigWithUpdatedDisabledHosts() {
    updateLock.lock();
    try {
      updateDisabledHosts();
      generateShardConfigThreadUnsafe();
    } finally {
      updateLock.unlock();
    }
  }

  private void generateShardConfigThreadUnsafe() {
    monitor.incrementConfigGeneratorCalledCount();
    Stopwatch stopwatch = Stopwatch.createStarted();


    List<String> resources = helixAdmin.getResourcesInCluster(clusterName);
    filterOutTaskResources(resources);

    Set<String> existingHosts = new HashSet<String>();

    // compose cluster config
    JSONObject config = new JSONObject();
    for (String resource : resources) {
      // Resources starting with PARTICIPANT_LEADER is for HelixCustomCodeRunner
      if (resource.startsWith("PARTICIPANT_LEADER")) {
        continue;
      }

      ExternalView externalView = helixAdmin.getResourceExternalView(clusterName, resource);
      if (externalView == null) {
        monitor.incrementConfigGeneratorNullExternalView();
        LOG.error("Failed to get externalView for resource: " + resource);
        // We must bypass a resource for which we get a null externalView.
        continue;
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

          if (disabledHosts.contains(entry.getKey())) {
            // exclude disabled hosts from the shard map config
            continue;
          }

          String state = entry.getValue();
          if (!state.equalsIgnoreCase("ONLINE") &&
              !state.equalsIgnoreCase("MASTER") &&
              !state.equalsIgnoreCase("SLAVE")) {
            // Only ONLINE, MASTER and SLAVE states are ready for serving traffic
            continue;
          }

          String hostWithDomain = getHostWithDomain(entry.getKey());
          List<String> partitionList = hostToPartitionList.get(hostWithDomain);
          if (partitionList == null) {
            partitionList = new ArrayList<String>();
            hostToPartitionList.put(hostWithDomain, partitionList);
          }

          if (state.equalsIgnoreCase("SLAVE")) {
            partitionList.add(partitionNumber + ":S");
          } else if (state.equalsIgnoreCase("MASTER")) {
            partitionList.add(partitionNumber + ":M");
          } else {
            partitionList.add(partitionNumber);
          }
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
      config.put(resource, resourceConfig);
    }

    // remove host that doesn't exist in the ExternalView from hostToHostWithDomain
    hostToHostWithDomain.keySet().retainAll(existingHosts);

    String newContent = config.toString();
    if (lastPostedContent != null && lastPostedContent.equals(newContent)) {
      LOG.error("Identical external view observed, skip updating config.");
      return;
    }

    // Write the shard config to local
    if (enableDumpToLocal) {
      try {
        FileWriter shard_config_writer = new FileWriter("/var/log/helixspectator/shard_config");
        shard_config_writer.write(newContent);
        shard_config_writer.close();
        LOG.error("Successfully wrote the shard config to the local.");
      } catch (IOException e) {
        LOG.error("An error occurred when writing shard config to local");
        e.printStackTrace();
      }
    }

    // Write the config to http service
    LOG.error("Generating a new shard config...");

    // If postUrl is not null and not empty, only then attemp to post newContent
    if (this.postUrl != null && !this.postUrl.isEmpty()) {
      Status status = doHttpPost(this.postUrl, this.dataParameters, newContent);
      if (status == Status.SUCCESS) {
        lastPostedContent = newContent;
        try {
          TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
          LOG.error("Sleep interrupted", e);
        }
      }
    }
    stopwatch.stop();
    long elapsedMs = stopwatch.elapsed(TimeUnit.MILLISECONDS);
    monitor.reportConfigGeneratorLatency(elapsedMs);
  }

  enum Status {
    SUCCESS,
    API_FAILURE,
    API_EXCEPTION,
  };

  private static Status doHttpPost(String postUrl, JSONObject dataParameters, String newContent) {
    dataParameters.remove("content");
    dataParameters.put("content", newContent);
    HttpPost httpPost = new HttpPost(postUrl);
    try {
      httpPost.setEntity(new StringEntity(dataParameters.toString()));
      HttpResponse response = new DefaultHttpClient().execute(httpPost);
      if (response.getStatusLine().getStatusCode() == 200) {
        return Status.SUCCESS;
      } else {
        LOG.error(response.getStatusLine().getReasonPhrase());
        return Status.API_FAILURE;
      }
    } catch (Exception e) {
      LOG.error("Failed to post the new config", e);
      return Status.API_EXCEPTION;
    }
  }

  private String getHostWithDomain(String host) {
    String hostWithDomain = hostToHostWithDomain.get(host);
    if (hostWithDomain != null) {
      return hostWithDomain;
    }

    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, host);
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
    HelixAdmin admin = helixManager.getClusterManagmentTool();

    Set<String> latestDisabledInstances = new HashSet<>(
        admin.getInstancesInClusterWithTag(clusterName, "disabled"));

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
      IdealState ideal = helixAdmin.getResourceIdealState(clusterName, res);
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
}
