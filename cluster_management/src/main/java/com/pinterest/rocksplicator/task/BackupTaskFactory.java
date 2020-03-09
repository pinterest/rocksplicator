package com.pinterest.rocksplicator.task;

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
 * An implementation of {@link TaskFactory} that create Tasks upload job launching.
 *
 * This class takes in {@param adminPort}, {@param cluster}, {@param zkClient} from
 * {@link com.pinterest.rocksplicator.Participant} used to identify job (tasks) belonging.
 * {@param zkClient} used to access zNode from zk cluster
 *
 * Upon Job creation, take the {@link JobConfig} instantiation time from {@link TaskCallbackContext}
 * as a universal version for tasks of the job.
 * jobVersion == resourceVersion
 *
 * Upon Task creation, take in params from JobConfig which could be passed at job creation;
 * JobCommandConfigMap: (optional)BACKUP_LIMIT_MBS, specify backup limit by mbs;
 * Upon Task execution, store resourceVersion, resourceCluster, jobVersion in userStore at Job level
 */

public class BackupTaskFactory implements TaskFactory {

  private static final Logger LOG = LoggerFactory.getLogger(BackupTaskFactory.class);

  private String host;
  private int adminPort;
  private String cluster;
  private CuratorFramework zkClient;

  private static final int DEFAULT_BACKUP_LIMIT_MBS = 64;

  public BackupTaskFactory(String host, int adminPort, String zkConnectString, String cluster) {
    this.host = host;
    this.cluster = cluster;
    this.adminPort = adminPort;
    this.zkClient = CuratorFrameworkFactory.newClient(zkConnectString,
        new ExponentialBackoffRetry(1000, 3));
    zkClient.start();
  }

  /**
   * Returns a {@link Task} instance.
   * @param context Contextual information for the task, including task and job configurations
   */
  @Override
  public Task createNewTask(TaskCallbackContext context) {

    TaskConfig taskConfig = context.getTaskConfig();
    JobConfig jobConfig = context.getJobConfig();

    LOG.error("Create task with TaskConfig: " + taskConfig.toString());

    // get job name and version; job name is namespaced
    String job = jobConfig.getJobId();
    long jobVersion = jobConfig.getStat().getCreationTime();

    // get backup upload rate limit
    int backupLimitMbs = DEFAULT_BACKUP_LIMIT_MBS;
    try {
      Map<String, String> jobCmdMap = jobConfig.getJobCommandConfigMap();
      if (jobCmdMap != null && !jobCmdMap.isEmpty()) {
        if (jobCmdMap.containsKey("BACKUP_LIMIT_MBS")) {
          backupLimitMbs = Integer.parseInt(jobCmdMap.get("BACKUP_LIMIT_MBS"));
        }
      }
    } catch (Exception e) {
      LOG.error("Failed to parse backup_limit_mbs from job command config map");
    }

    // get target partition and resource name
    String targetPartition = taskConfig.getTargetPartition();
    String resourceName = targetPartition.substring(0, targetPartition.lastIndexOf("_"));

    LOG.error(String.format(
        "Create Task for cluster: %s, targetPartition: %s from job: %s at jobVersion: %d to "
            + "execute at host: %s, port: %d", cluster, targetPartition, job, jobVersion, host,
        adminPort));

    return getTask(cluster, resourceName, targetPartition, backupLimitMbs, jobVersion, job, host,
        adminPort, zkClient);
  }

  protected Task getTask(String cluster, String resourceName, String targetPartition,
                         int backupLimitMbs, long jobVersion, String job,
                         String host, int port, CuratorFramework zkClient) {
    return new BackupTask(cluster, resourceName, targetPartition, backupLimitMbs, jobVersion, job,
        host, port, zkClient);
  }

} 