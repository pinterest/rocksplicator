package com.pinterest.rocksplicator;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * this class provide an abstract class to generate shard maps (and job state maps when helix
 * task framework adopted).
 *
 * extends {@link RoutingTableProvider} enable generator as listener to ExternalViewChange,
 * InstanceChange, ConfigChange, LiveInstanceChange, and CurrentStateChange; implements
 * {@link CustomCodeCallbackHandler} enable generator to run customized code in callback when
 * listening to changes
 */

public abstract class Generator extends RoutingTableProvider implements CustomCodeCallbackHandler {

  private static final Logger LOG = LoggerFactory.getLogger(Generator.class);

  protected final String clusterName;
  protected HelixManager helixManager;
  protected Map<String, String> hostToHostWithDomain;
  protected Set<String> disabledHosts;
  protected JSONObject dataParameters;
  protected Map<String, String> lastPostedContents;

  public Generator(String clusterName, HelixManager helixManager) {
    this.clusterName = clusterName;
    this.helixManager = helixManager;
    this.hostToHostWithDomain = new HashMap<>();
    this.disabledHosts = new HashSet<>();
    this.dataParameters = new JSONObject();
    this.dataParameters.put("config_version", "v3");
    this.dataParameters.put("author", "Generator");
    this.dataParameters.put("comment", "new config");
    this.dataParameters.put("content", "{}");
    this.lastPostedContents = new HashMap<>();
  }

  /**
   * generate db resources shard map
   */
  public String getShardConfig() {
    HelixAdmin admin = helixManager.getClusterManagmentTool();

    // get the resource from idealstate
    List<String> resources = admin.getResourcesInCluster(clusterName);

    Set<String> existingHosts = new HashSet<String>();

    // compose cluster config
    JSONObject config = new JSONObject();
    for (String resource : resources) {
      // Resources starting with PARTICIPANT_LEADER is for HelixCustomCodeRunner
      if (resource.startsWith("PARTICIPANT_LEADER")) {
        continue;
      }

      LOG.error(
          "Config generator try to get partitions from externalView for resource: " + resource);

      ExternalView externalView = admin.getResourceExternalView(clusterName, resource);
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

    return config.toString();
  }

  public String getHostWithDomain(String host) {
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


  /**
   * update disabledHosts, return true if there is any changes
   *
   * when host tagged with "disabled", new config generate to exclude the disabled host.
   * However, if cluster is in maintenance mode, no new config generate, need first clean up
   * disabled hosts, then, disable maintenance mode to rebalance resources.
   * TODO: if cluster is not in maintenance mode, "disabled" hosts have already been excluded
   * during resource rebalance; shouldn't rebalance again upon "disabled" hosts clean up.
   */
  public boolean updateDisabledHosts() {
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
   * post generated config into url and update cache accordingly
   */
  public void postToUrl(String newContent, String postUrl) {

    if (lastPostedContents != null && lastPostedContents.containsKey(postUrl) && lastPostedContents
        .get(postUrl).equals(newContent)) {
      LOG.error("Identical external view observed, skip updating config.");
      return;
    }

    // Write the config to ZK
    LOG.error("Generating a new job state or config to " + postUrl);

    this.dataParameters.remove("content");
    this.dataParameters.put("content", newContent);
    HttpPost httpPost = new HttpPost(postUrl);
    try {
      httpPost.setEntity(new StringEntity(this.dataParameters.toString()));
      HttpResponse response = new DefaultHttpClient().execute(httpPost);
      if (response.getStatusLine().getStatusCode() == 200) {
        lastPostedContents.put(postUrl, newContent);
        LOG.error("Succeed to generate a new shard config, sleep for 2 seconds");
        TimeUnit.SECONDS.sleep(2);
      } else {
        LOG.error(response.getStatusLine().getReasonPhrase());
      }
    } catch (Exception e) {
      LOG.error("Failed to post the new config", e);
    }
  }

}
