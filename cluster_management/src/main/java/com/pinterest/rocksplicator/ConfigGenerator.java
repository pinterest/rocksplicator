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

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.CustomCodeCallbackHandler;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConfigGenerator implements CustomCodeCallbackHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigGenerator.class);

  private final String clusterName;
  private HelixManager helixManager;
  private Map<String, String> hostToHostWithDomain;
  private final String postUrl;
  private JSONObject dataParameters;
  private String lastPostedContent;

  public ConfigGenerator(String clusterName, HelixManager helixManager, String configPostUrl) {
    this.clusterName = clusterName;
    this.helixManager = helixManager;
    this.hostToHostWithDomain = new HashMap<String, String>();
    this.postUrl = configPostUrl;
    this.dataParameters = new JSONObject();
    this.dataParameters.put("config_version", "v3");
    this.dataParameters.put("author", "ConfigGenerator");
    this.dataParameters.put("comment", "new shard config");
    this.dataParameters.put("content", "{}");
    this.lastPostedContent = null;
  }

  @Override
  public void onCallback(NotificationContext notificationContext) {
    LOG.info("Received notification: " + notificationContext.getChangeType());

    if (notificationContext.getChangeType() == HelixConstants.ChangeType.EXTERNAL_VIEW) {
      generateShardConfig();
    } else if (notificationContext.getChangeType() == HelixConstants.ChangeType.LIVE_INSTANCE) {
      generateIdealState();
    }
  }

  private void generateShardConfig() {
    HelixAdmin admin = helixManager.getClusterManagmentTool();

    List<String> resources = admin.getResourcesInCluster(clusterName);

    Set<String> existingHosts = new HashSet<String>();

    // compose cluster config
    JSONObject config = new JSONObject();
    for (String resource : resources) {
      // Resources starting with PARTICIPANT_LEADER is for HelixCustomCodeRunner
      if (resource.startsWith("PARTICIPANT_LEADER")) {
        continue;
      }

      ExternalView externalView = admin.getResourceExternalView(clusterName, resource);
      Set<String> partitions = externalView.getPartitionSet();

      // compose resource config
      JSONObject resourceConfig = new JSONObject();
      String partitionsStr = externalView.getRecord().getSimpleField("NUM_PARTITIONS");
      resourceConfig.put("num_shards", Integer.parseInt(partitionsStr));

      // build host to partition list map
      Map<String, List<String>> hostToPartitionList = new HashMap<String, List<String>>();
      for (String partition : partitions) {
        String partitionNumber =
            String.format("%05d", Integer.parseInt(partition.split("_")[1]));
        Map<String, String> hostToState = externalView.getStateMap(partition);
        for (Map.Entry<String, String> entry : hostToState.entrySet()) {
          existingHosts.add(entry.getKey());

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
      LOG.info("Identical external view observed, skip updating config.");
      return;
    }

    // Write the config to ZK
    LOG.info("Generating a new shard config...");

    this.dataParameters.remove("content");
    this.dataParameters.put("content", newContent);
    HttpPost httpPost = new HttpPost(this.postUrl);
    try {
      httpPost.setEntity(new StringEntity(this.dataParameters.toString()));
      HttpResponse response = new DefaultHttpClient().execute(httpPost);
      if (response.getStatusLine().getStatusCode() == 200) {
        lastPostedContent = newContent;
        LOG.info("Succeed to generate a new shard config.");
      } else {
        LOG.error(response.getStatusLine().getReasonPhrase());
      }
    } catch (Exception e) {
      LOG.error("Failed to post the new config", e);
    }
  }

  private String getHostWithDomain(String host) {
    String hostWithDomain = hostToHostWithDomain.get(host);
    if (hostWithDomain != null) {
      return hostWithDomain;
    }

    // local cache missed, read from ZK
    HelixAdmin admin = helixManager.getClusterManagmentTool();
    InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, host);
    String domain = instanceConfig.getDomain();
    String[] parts = domain.split(",");
    String az = parts[0].split("=")[1];
    String pg = parts[1].split("=")[1];
    hostWithDomain = host.replace('_', ':') + ":" + az + "_" + pg;
    hostToHostWithDomain.put(host, hostWithDomain);
    return hostWithDomain;
  }

  private void generateIdealState() {
    // TODO
  }
}
