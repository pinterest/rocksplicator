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
// @author kangnanli (kangnanli@pinterest.com)
//

package com.pinterest.rocksplicator.task;

import com.pinterest.rocksplicator.Utils;

import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * backup a single local db shard to cloud
 */
public class BackupTask extends UserContentStore implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(BackupTask.class);

  private final String taskCluster;
  private final String partitionName;
  private final int backupLimitMbs;
  private final String storePathPrefix;
  private final long resourceVersion;
  private final String job;
  private final int adminPort;
  private final boolean useS3Store;
  private final String s3Bucket;
  private final boolean shareFilesWithChecksum;
  private final TaskConfig taskConfig;

  public BackupTask(String taskCluster, String partitionName, int backupLimitMbs,
                    String storePathPrefix, long resourceVersion, String job, int adminPort,
                    boolean useS3Store, String s3Bucket, boolean shareFilesWithChecksum,
                    TaskConfig taskConfig) {
    this.taskCluster = taskCluster;
    this.partitionName = partitionName;
    this.backupLimitMbs = backupLimitMbs;
    this.storePathPrefix = storePathPrefix;
    this.resourceVersion = resourceVersion;
    this.job = job;
    this.adminPort = adminPort;
    this.useS3Store = useS3Store;
    this.s3Bucket = s3Bucket;
    this.shareFilesWithChecksum = shareFilesWithChecksum;
    this.taskConfig = taskConfig;
  }

  /**
   * Execute the task.
   * @return A {@link TaskResult} object indicating the status of the task and any additional
   *         context information that can be interpreted by the specific {@link Task}
   *         implementation.
   */
  @Override
  public TaskResult run() {

    String dbName = Utils.getDbName(partitionName);
    if (storePathPrefix.isEmpty()) {
      String errMsg =
          String.format(
              "Cancel the task, storePathPrefix is not provided from job command config map. "
                  + "taskConfig=%s",
              taskConfig.toString());
      LOG.error(errMsg);
      return new TaskResult(TaskResult.Status.CANCELED, errMsg);
    }

    try {
      String storePath =
          String.format("%s/%s/%s", storePathPrefix, String.valueOf(resourceVersion), dbName);

      LOG.error(
          String.format(
              "BackupTask run to backup partition: %s to storePath: %s. Other info {taskCluster: "
                  + "%s, job: %s, version: %d, taskConfig=%s}", partitionName, storePath,
              taskCluster, job, resourceVersion, taskConfig.toString()));

      executeBackup("127.0.0.1", adminPort, dbName, storePath, backupLimitMbs, useS3Store,
          s3Bucket, shareFilesWithChecksum);

      return new TaskResult(TaskResult.Status.COMPLETED, "BackupTask is completed!");
    } catch (Exception e) {
      String errMsg =
          String
              .format("Task backup failed. errMsg=%s. stacktrace=%s. taskConfig=%s", e.getMessage(),
                  Arrays.toString(e.getStackTrace()), taskConfig.toString());
      LOG.error(errMsg);
      return new TaskResult(TaskResult.Status.FAILED, errMsg);
    }

  }

  protected void executeBackup(String host, int port, String dbName, String storePath,
                               int backupLimitMbs, boolean useS3Store, String s3Bucket,
                               boolean shareFilesWithChecksum)
      throws RuntimeException {
    try {
      if (useS3Store) {
        Utils.backupDBToS3WithLimit(host, port, dbName, backupLimitMbs, s3Bucket, storePath,
            shareFilesWithChecksum);
      } else {
        Utils.backupDBWithLimit(host, port, dbName, storePath, backupLimitMbs,
            shareFilesWithChecksum);
      }
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
    // TODO: delete the db from cloud when cancel
    // bakcup might be interrupted by host unreachable or termination or issued cancellation; after
    // cancel, partial backup files remained on cloud by rocksdb backupEngine.
    // Two options: 1. remove unfinished shard, so that new backup can start over; 2. leave
    // untouched, if Task resume on same replica, then, wont re-upload those files, if Task
    // resume on another replica, will ignore(?) leftovers since checksum of files are verified

    // options2: leave leftovers untouched
    LOG.error(String.format("BackupTask cancelled. taskConfig=%s", taskConfig.toString()));
  }
}
