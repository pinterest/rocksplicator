package com.pinterest.rocksplicator.task;

import com.pinterest.rocksplicator.Utils;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DedupTask extends UserContentStore implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(DedupTask.class);

  private final String srcStorePathPrefix;
  private final long resourceVersion;
  private final String partitionName;
  private final String taskCluster;
  private final String job;
  private final int adminPort;
  private final String destStorePathPrefix;
  private final boolean useS3Store;
  private final String s3Bucket;
  private final int backupLimitMbs;
  private final boolean shareFilesWithChecksum;

  public DedupTask(String srcStorePathPrefix, long resourceVersion, String partitionName,
                   String taskCluster, String job, int adminPort,
                   String destStorePathPrefix, boolean useS3Store, String s3Bucket,
                   int backupLimitMbs, boolean shareFilesWithChecksum) {
    this.srcStorePathPrefix = srcStorePathPrefix;
    this.resourceVersion = resourceVersion;
    this.partitionName = partitionName;
    this.taskCluster = taskCluster;
    this.job = job;
    this.adminPort = adminPort;
    this.destStorePathPrefix = destStorePathPrefix;
    this.useS3Store = useS3Store;
    this.s3Bucket = s3Bucket;
    this.shareFilesWithChecksum = shareFilesWithChecksum;
    this.backupLimitMbs = backupLimitMbs;
  }

  /**
   * Execute the task.
   * @return A {@link TaskResult} object indicating the status of the task and any additional
   *         context information that can be interpreted by the specific {@link Task}
   *         implementation.
   */
  @Override
  public TaskResult run() {

    if (srcStorePathPrefix.isEmpty() || resourceVersion == -1 || destStorePathPrefix.isEmpty()) {
      String errMsg =
          "Cancel the task, due to job command config map failed to provide all three of "
              + "srcStorePathPrefix, resourceVersion, and destStorePathPrefix";
      LOG.error(errMsg);
      return new TaskResult(TaskResult.Status.CANCELED, errMsg);
    }

    // check task created with resource name and match prefix of target partition
    String dbName = Utils.getDbName(partitionName);

    try {

      String srcStorePath =
          String.format("%s/%s/%s", srcStorePathPrefix, String.valueOf(resourceVersion), dbName);

      String destStorePath =
          String.format("%s/%s/%s", destStorePathPrefix, String.valueOf(resourceVersion), dbName);

      LOG.error(
          String.format(
              "DedupTask run to dedup partition: %s from source path: %s, to dest path: %s "
                  + "Other info {cluster: %s, job: %s, resourceVersion: %d}", dbName,
              srcStorePath, destStorePath, taskCluster, job, resourceVersion));

      executeDedup(dbName, adminPort, srcStorePath, destStorePath, useS3Store, s3Bucket,
          backupLimitMbs, shareFilesWithChecksum);

      LOG.error("DedupTask completed, with: success");
      return new TaskResult(TaskResult.Status.COMPLETED, "DedupTask is completed!");
    } catch (Exception e) {
      LOG.error("Task dedup failed", e);
      return new TaskResult(TaskResult.Status.FAILED, "DedupTask failed");
    }

  }

  protected void executeDedup(String dbName, int adminPort, String srcStorePath,
                              String destStorePath, boolean useS3Store, String s3Bucket,
                              int backupLimitMbs, boolean shareFilesWithChecksum)
      throws RuntimeException {
    try {
      Utils.addDB(dbName, adminPort, "SLAVE");
      Utils.closeRemoteOrLocalDB("localhost", adminPort, dbName);
      if (useS3Store) {
        Utils.restoreLocalDBFromS3(adminPort, dbName, s3Bucket, srcStorePath, "127.0.0.1",
            adminPort);
      } else {
        Utils.restoreLocalDB(adminPort, dbName, srcStorePath, "127.0.0.1", adminPort);
      }
      LOG.error("restoreDB is done, begin compactDB");

      Utils.compactDB(adminPort, dbName);
      LOG.error("compactDB is done");
      if (useS3Store) {
        Utils.backupDBToS3WithLimit("127.0.0.1", adminPort, dbName, backupLimitMbs, s3Bucket,
            destStorePath);
      } else {
        Utils.backupDBWithLimit("127.0.0.1", adminPort, dbName, destStorePath, backupLimitMbs,
            shareFilesWithChecksum);
      }
      Utils.clearDB(dbName, adminPort);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Signals the task to stop execution. The task implementation should carry out any clean up
   * actions that may be required and return from the {@link #run()} method.
   *
   * with default TaskStateModel, "cancel()" invoked by {@link org.apache.helix.task.TaskRunner
   * #cancel()} during state transitions: running->stopped /task_aborted /dropped /init, and during
   * {@link org.apache.helix.task.TaskStateModel #reset()}
   */
  @Override
  public void cancel() {
    // upon cancel, clear db from local
    String dbName = Utils.getDbName(partitionName);
    Utils.clearDB(dbName, adminPort);
    LOG.error("DedupTask cancelled");
  }
}