package com.pinterest.rocksplicator.task;

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
 * This class takes in {@param cluster}, {@param adminPort}, from
 * {@link com.pinterest.rocksplicator.Participant} used to identify job (tasks) belonging.
 *
 * Upon Task creation, take in params from JobConfig which could be passed at job creation.
 * JobCommandConfigMap: SRC_STORE_PATH_PREFIX, RESOURCE_VERSION, DEST_STORE_PATH_PREFIX
 * Upon Task execution, download db from "src_prefix/version/dbName", dedup, upload to
 * "dest_prefix/version/dbName"
 */

public class DedupTaskFactory implements TaskFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DedupTaskFactory.class);

  private final String cluster;
  private final int adminPort;
  private final boolean useS3Store;
  private final String s3Bucket;

  private static final int DEFAULT_BACKUP_LIMIT_MBS = 64;

  public DedupTaskFactory(String cluster, int adminPort, boolean useS3Store, String s3Bucket) {
    this.cluster = cluster;
    this.adminPort = adminPort;
    this.useS3Store = useS3Store;
    this.s3Bucket = s3Bucket;
  }

  /**
   * @param context Contextual information for the task, including task and job configurations
   * @return A {@link Task} instance.
   */
  @Override
  public Task createNewTask(TaskCallbackContext context) {

    TaskConfig taskConfig = context.getTaskConfig();
    JobConfig jobConfig = context.getJobConfig();
    String job = jobConfig.getJobId();

    String srcStorePathPrefix = "";
    long resourceVersion = -1;
    String destStorePathPrefix = "";
    int backupLimitMbs = DEFAULT_BACKUP_LIMIT_MBS;
    boolean shareFilesWithChecksum = false;

    try {
      Map<String, String> jobCmdMap = jobConfig.getJobCommandConfigMap();
      if (jobCmdMap != null && !jobCmdMap.isEmpty()) {
        if (jobCmdMap.containsKey("SRC_STORE_PATH_PREFIX")) {
          srcStorePathPrefix = jobCmdMap.get("SRC_STORE_PATH_PREFIX");
        }
        if (jobCmdMap.containsKey("RESOURCE_VERSION")) {
          resourceVersion = Long.parseLong(jobCmdMap.get("RESOURCE_VERSION"));
        }
        if (jobCmdMap.containsKey("DEST_STORE_PATH_PREFIX")) {
          destStorePathPrefix = jobCmdMap.get("DEST_STORE_PATH_PREFIX");
        }
        if (jobCmdMap.containsKey("BACKUP_LIMIT_MBS")) {
          backupLimitMbs = Integer.parseInt(jobCmdMap.get("BACKUP_LIMIT_MBS"));
        }
        if (jobCmdMap.containsKey("ROCKSDB_SHARE_FILES_WITH_CHECKSUM")) {
          shareFilesWithChecksum =
              Boolean.parseBoolean(jobCmdMap.get("ROCKSDB_SHARE_FILES_WITH_CHECKSUM"));
        }
      }
    } catch (NumberFormatException e) {
      LOG.error("Failed to parse resource_version from job command config map", e);
    }

    String targetPartition = taskConfig.getTargetPartition();

    LOG.error(String.format(
        "Create Task for cluster: %s, targetPartition: %s from job: %s to execute at localhost, "
            + "port: %d. {resourceVersion: %d, helixJobCreationTime: %d, taskCreationTime: %d, "
            + "taskConfig: %s}", cluster, targetPartition, job, adminPort, resourceVersion,
        jobConfig.getStat().getCreationTime(), System.currentTimeMillis(), taskConfig.toString()));

    return getTask(srcStorePathPrefix, resourceVersion, targetPartition, cluster, job, adminPort,
        destStorePathPrefix, useS3Store, s3Bucket, backupLimitMbs, shareFilesWithChecksum,
        taskConfig);
  }

  protected DedupTask getTask(String srcStorePathPrefix, long resourceVersion, String partitionName,
                              String cluster, String job, int port, String destStorePathPrefix,
                              boolean useS3Store, String s3Bucket, int backupLimitMbs,
                              boolean shareFilesWithChecksum, TaskConfig taskConfig) {
    return new DedupTask(srcStorePathPrefix, resourceVersion, partitionName, cluster, job, port,
        destStorePathPrefix, useS3Store, s3Bucket, backupLimitMbs, shareFilesWithChecksum,
        taskConfig);
  }

}