package com.pinterest.rocksplicator.task;

import com.pinterest.rocksplicator.TaskUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * an implementation of {@link TaskFactory} that creates Tasks to dedup rocksdb instances
 *
 * This class takes in {@param adminPort}, {@param cluster}, {@param zkClient} from
 * {@link com.pinterest.rocksplicator.Participant} used to identify job (tasks) belonging.
 * {@param zkClient} used to access zNode from zk cluster
 *
 * tasks get the source data version and download to local and execute compaction, backup to s3.
 * two options to get source data version: 1. get the specified version from TaskConfig, suitable
 * for ad-hoc tasks; 2. get the latest backup-completed version from KVManager service, suitable
 * for recurrent scheduled tasks.
 * for 2: each Task will query KVManager independently, different versions might return. thus,
 * tasks will use jobVersion to create a zk lock and update the source data version to job
 * userContentStore. The 1st Task get the lock will update the source data version, other tasks
 * will use this version to replace their queried source data version.
 * job version zk lock: /[some_prefix]/rocksplicator/[task_cluster]/[src_cluster]/segment
 * /jobVersion/lock
 *
 * jobVersion != resourceVersion
 *
 * Upon Task creation, take in params from JobConfig which could be passed at job creation.
 * JobCommandConfigMap: RESOURCE_CLUSTER, RESOURCE, (optional)RESOURCE_VERSION
 * Upon Task execution, store resourceVersion, jobVersion, resourceCluster into Job level userStore
 */

public class DedupTaskFactory implements TaskFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DedupTaskFactory.class);

  private final String host;
  private final int adminPort;
  private final String cluster;
  private CuratorFramework zkClient;

  public DedupTaskFactory(String host, int adminPort, String zkConnectString, String cluster) {
    this.host = host;
    this.adminPort = adminPort;
    this.cluster = cluster;
    this.zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
        new ExponentialBackoffRetry(1000, 3));
    zkClient.start();
  }

  /**
   * @param context Contextual information for the task, including task and job configurations
   * @return A {@link Task} instance.
   */
  @Override
  public Task createNewTask(TaskCallbackContext context) {

    // TaskConfig: task_id, task_cmd (the cmd to run, e.g. rm), target_partition, configMap
    TaskConfig taskConfig = context.getTaskConfig();
    JobConfig jobConfig = context.getJobConfig();

    LOG.error("Create task with TaskConfig: " + taskConfig.toString());

    // get job name and version
    String job = jobConfig.getJobId();
    long jobVersion = jobConfig.getStat().getCreationTime();

    // get target partition
    String targetPartition = taskConfig.getTargetPartition();

    // get resource level info from jobConfig
    String resourceCluster = "";
    String resourceName = "";
    long resourceVersion = -1;

    try {

      Map<String, String> jobCmdMap = jobConfig.getJobCommandConfigMap();
      if (jobCmdMap != null && !jobCmdMap.isEmpty()) {
        if (jobCmdMap.containsKey("RESOURCE_CLUSTER")) {
          resourceCluster = jobCmdMap.get("RESOURCE_CLUSTER");
        }
        if (jobCmdMap.containsKey("RESOURCE")) {
          resourceName = jobCmdMap.get("RESOURCE");
        }
        if (jobCmdMap.containsKey("RESOURCE_VERSION")) {
          resourceVersion = Long.parseLong(jobCmdMap.get("RESOURCE_VERSION"));
        }
      }

      if (resourceVersion == -1) {
        // if no version provided, e.g. recurrent job
        // TODO: get the resource latest version from KVManager service
        resourceVersion = getLatestVersion(resourceCluster, resourceName);
      }

    } catch (NumberFormatException e) {
      LOG.error("Failed to parse resource_version from job command config map", e);
    } catch (Exception e) {
      LOG.error("CreateNewTask failed, fail to get resource version for task for partition: "
          + targetPartition);
    }

    LOG.error(String.format(
        "Create Task for cluster: %s, targetPartition: %s from job: %s at jobVersion: %d to "
            + "execute at host: %s, port: %d", cluster, targetPartition, job, jobVersion, host,
        adminPort));

    return getTask(cluster, resourceCluster, resourceName, targetPartition, resourceVersion,
        jobVersion, job, host, adminPort, zkClient);
  }

  protected Task getTask(String cluster, String resourceCluster, String resourceName,
                         String targetPartition,
                         long resourceVersion, long jobVersion, String job,
                         String host, int port, CuratorFramework zkClient) {
    return new DedupTask(cluster, resourceCluster, resourceName, targetPartition, resourceVersion,
        jobVersion, job, host, port, zkClient);
  }

  // separate the static call for testing mock; not able to directly mock TaskUtils
  // .getLatestResourceVersion since Factory registered @BeforeClass during test
  protected long getLatestVersion(String cluster, String resource) {
    return TaskUtils.getLatestResourceVersion(cluster, resource);
  }
}
