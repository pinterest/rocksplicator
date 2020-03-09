package com.pinterest.rocksplicator;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.CustomCodeCallbackHandler;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public abstract class Generator extends RoutingTableProvider implements CustomCodeCallbackHandler {

  private static final Logger LOG = LoggerFactory.getLogger(Generator.class);

  protected final String clusterName;
  protected HelixManager helixManager;
  protected TaskDriver driver;
  protected Map<String, String> hostToHostWithDomain;
  protected Set<String> disabledHosts;
  protected JSONObject dataParameters;
  protected Map<String, String> lastPostedContents;

  public Generator(String clusterName, HelixManager helixManager) {
    this.clusterName = clusterName;
    this.helixManager = helixManager;
    this.driver = null;
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
    List<String> db_resources = filterOutWfAndJobs(resources);

    Set<String> existingHosts = new HashSet<String>();

    // compose cluster config
    JSONObject config = new JSONObject();
    for (String resource : db_resources) {
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
   * filter out workflows and jobs from helix resources, only return db resources from ideal states
   *
   * db_resource and wf are guaranteed to be unique, bcz wf is treated as "resource" in helix,
   * no db_resource and wf are allowed with same name during db_resource or wf creation
   */
  public List<String> filterOutWfAndJobs(List<String> resources) {
    List<String> db_resources = new ArrayList<>();

    Map<String, WorkflowConfig> workflowConfigMap = (driver == null ? null : driver.getWorkflows());

    if (driver != null && workflowConfigMap != null && !workflowConfigMap.isEmpty()) {
      Set<String> workflows = workflowConfigMap.keySet();
      for (String resource : resources) {
        boolean isWfOrJob = false;
        for (String workflow : workflows) {
          if (!workflow.isEmpty() && resource.startsWith(workflow)) {
            isWfOrJob = true;
            break;
          }
        }
        if (!isWfOrJob) {
          db_resources.add(resource);
        }
      }
    } else {
      db_resources.addAll(resources);
    }

    return db_resources;
  }

  /**
   * generate job status of resource versions
   *
   * currently, for those resource versions still managed by the cluster (workflow will be removed
   * after completion in 2 days, thus only resource versions in 2 days will be reported), we use
   * ConfigV3 managedhashmap to store the state map of resource versions.
   *
   * it will go through each workflow, and for each job in workflow: get job version, resource
   * name, resource cluster from job config; get resource version from Job level userStore; get
   * job state from job context through driver.
   * And, generate state map:
   * resourceName -> {resourceVersion -> {jobVersion -> jobState}}
   *
   * store jobVersion along with resourceVersion, bcz diff versions of export job might try to
   * export the version of resource. This is not allowed unless old version of export job failed.
   */
  public String getJobStateConfig() {

    // after job creation, the wf state generator won't be triggered immediately, thus, we can't
    // have
    // a "SCHEDULED/NOT_STARTED" state for resource-version.
    // TODO: need to listen to other change besides EXTERNAL_VIEW

    Map<String, Map<String, Map<String, String>>> stateMap = new HashMap<>();

    Map<String, WorkflowConfig> workflowConfigMap = (driver == null ? null : driver.getWorkflows());

    if (workflowConfigMap != null) {
      for (String wf : workflowConfigMap.keySet()) {
        WorkflowConfig wfConfig = workflowConfigMap.get(wf);
        WorkflowContext wfContext = driver.getWorkflowContext(wf);

        if (wfConfig != null && wfContext != null) {
          Set<String> jobs = wfConfig.getJobDag().getAllNodes();

          for (String job : jobs) {
            // each job is responsible to "action" on one version of a resource

            // job is already namespaced from WorkflowConfig
            JobConfig jobConfig = driver.getJobConfig(job);

            // get resource
            Map<String, String> jobCmdMap = jobConfig.getJobCommandConfigMap();
            String resourceName;
            if (jobCmdMap != null && !jobCmdMap.isEmpty()) {
              resourceName = jobCmdMap.get("RESOURCE");
            } else {
              LOG.error(
                  String.format("JobCommandConfig for job: %s does not contain RESOURCE", job));
              continue;
            }
            if (!stateMap.containsKey(resourceName)) {
              stateMap.put(resourceName, new HashMap<String, Map<String, String>>());
            }

            // get jobVersion
            String jobVersion = String.valueOf(jobConfig.getStat().getCreationTime());

            // get resourceVersion
            // resource version default to be job version, unless Job level userStore provide
            // different resource version, i.e during dedupJob execution, store source resource
            // version and own job version into Job level userStore
            String resourceVersion = jobVersion;
            String jobDeName = TaskUtil.getDenamespacedJobName(wf, job);
            Map<String, String> contentMap = driver.getJobUserContentMap(wf, jobDeName);
            if (contentMap != null && !contentMap.isEmpty() && contentMap
                .containsKey("resourceVersion")) {
              // useStore only available after 1 task begin to run
              String userStoreResourceVersion = contentMap.get("resourceVersion");
              if (!userStoreResourceVersion.isEmpty() && !resourceVersion
                  .equals(userStoreResourceVersion)) {
                LOG.error(
                    String.format(
                        "resourceVersion {%s} from job userStore mismatch resourceVersion"
                            + "(jobVersion) {%s} from jobConfig, use resourceVersion from "
                            + "userStore",
                        userStoreResourceVersion, resourceVersion));
                resourceVersion = userStoreResourceVersion;
              }
              // String resourceCluster = contentMap.get("resourceCluster");
            } else {
              LOG.error("Can't get userContent for DeNameSpaced job: " + jobDeName);
            }

            // get state
            TaskState jobState = wfContext.getJobState(job);

            if (!stateMap.get(resourceName).containsKey(resourceVersion)) {
              stateMap.get(resourceName).put(resourceVersion, new HashMap<String, String>());
            }

            // map TaskState enum to Backup/Export Status enum
            if (jobState == null) {
              // no jobState found for the job, default to be NOT_STARTED
              // FIXME: could probably mean in pending, not scheduled yet
              LOG.error(String.format("JobState is not found. Job: %s is probably cancelled", job));
              stateMap.get(resourceName).get(resourceVersion).put(jobVersion, "NOT_STARTED");
            } else if (jobState == TaskState.STOPPED) {
              // map jobState (STOPPED) to backup/dedup state PAUSED
              stateMap.get(resourceName).get(resourceVersion).put(jobVersion, "PAUSED");
            } else if (jobState == TaskState.ABORTED || jobState == TaskState.TIMED_OUT
                || jobState == TaskState.TIMING_OUT || jobState == TaskState.FAILING
                || jobState == TaskState.FAILED) {
              stateMap.get(resourceName).get(resourceVersion).put(jobVersion, "FAILED");
            } else {
              stateMap.get(resourceName).get(resourceVersion).put(jobVersion, jobState.name());
            }

          }
        } else {
          // no workflow context or config found
          LOG.error(String.format(
              "WorkflowContext or WorkflowConfig is not found. Workflow: %s is probably cancelled",
              wf));
        }
      }
    }
    // compare curt clusterStateReport to last
    return JSONValue.toJSONString(stateMap);
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

