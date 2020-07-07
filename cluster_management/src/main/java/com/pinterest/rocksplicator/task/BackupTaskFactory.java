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

import org.apache.helix.task.JobConfig;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * An implementation of {@link TaskFactory} that create Tasks upon backup-job launching.
 *
 * This class takes in {@param adminPort}, {@param cluster}, from
 * {@link com.pinterest.rocksplicator.Participant} to create tasks to backup local db to cloud.
 **
 * Upon Task creation, configs inherited from job configs JobCommandConfigMap: STORE_PATH_PREFIX,
 * the cloud path prefix to store the backup; RESOURCE_VERSION: the timestamp the backup job is
 * submitted from scheduling service, if not provided, use the job creation time as the default
 * resource version; "prefix/[version]/[dbName]" will be the full filesystem path to store db on
 * cloud; (optional) BACKUP_LIMIT_MBS, specify backup limit by mbs;
 */
public class BackupTaskFactory implements TaskFactory {

  private static final Logger LOG = LoggerFactory.getLogger(BackupTaskFactory.class);

  private final String cluster;
  private final int adminPort;
  private final boolean useS3Store;
  private final String s3Bucket;

  private static final int DEFAULT_BACKUP_LIMIT_MBS = 64;

  public BackupTaskFactory(String cluster, int adminPort, boolean useS3Store, String s3Bucket) {
    this.cluster = cluster;
    this.adminPort = adminPort;
    this.useS3Store = useS3Store;
    this.s3Bucket = s3Bucket;
  }

  /**
   * Returns a {@link Task} instance.
   * @param context Contextual information for the task, including task and job configurations
   */
  @Override
  public Task createNewTask(TaskCallbackContext context) {

    TaskConfig taskConfig = context.getTaskConfig();
    JobConfig jobConfig = context.getJobConfig();
    String job = jobConfig.getJobId();

    LOG.error("Create task with TaskConfig: " + taskConfig.toString());

    long jobCreationTime = jobConfig.getStat().getCreationTime();

    String storePathPrefix = "";
    long resourceVersion = jobCreationTime;
    int backupLimitMbs = DEFAULT_BACKUP_LIMIT_MBS;
    boolean shareFilesWithChecksum = true;

    try {
      Map<String, String> jobCmdMap = jobConfig.getJobCommandConfigMap();
      if (jobCmdMap != null && !jobCmdMap.isEmpty()) {
        if (jobCmdMap.containsKey("STORE_PATH_PREFIX")) {
          storePathPrefix = jobCmdMap.get("STORE_PATH_PREFIX");
        }
        if (jobCmdMap.containsKey("RESOURCE_VERSION")) {
          resourceVersion = Long.parseLong(jobCmdMap.get("RESOURCE_VERSION"));
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
      LOG.error(
          String.format(
              "Failed to parse configs from job command config map, use defaults backupLimitMbs: "
                  + "%d, resourceVersion: %d", DEFAULT_BACKUP_LIMIT_MBS, resourceVersion), e);
    }

    String targetPartition = taskConfig.getTargetPartition();

    LOG.error(
        String.format(
            "Create Task for cluster: %s, targetPartition: %s from job: %s to execute at "
                + "localhost, port: %d. {resourceVersion: %d, helixJobCreationTime: %d, "
                + "taskCreationTime: %d}",
            cluster, targetPartition, job, adminPort, resourceVersion, jobCreationTime,
            System.currentTimeMillis()));

    return getTask(cluster, targetPartition, backupLimitMbs, storePathPrefix, resourceVersion, job,
        adminPort, useS3Store, s3Bucket, shareFilesWithChecksum);
  }

  protected Task getTask(String cluster, String targetPartition, int backupLimitMbs,
                         String storePathPrefix, long resourceVersion, String job, int port,
                         boolean useS3Store, String s3Bucket, boolean shareFilesWithChecksum) {
    return new BackupTask(cluster, targetPartition, backupLimitMbs, storePathPrefix,
        resourceVersion, job, port, useS3Store, s3Bucket, shareFilesWithChecksum);
  }

}
