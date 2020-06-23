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

import org.apache.helix.HelixAdmin;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * An implementation of {@link TaskFactory} that create Tasks to restore backed up data from
 * cloud to local.
 *
 * There are 2 use cases: 1. if periodical backup is setup for the resource, then, RestoreTask
 * provide rollback to a previous backup version, especially during disaster recovery.
 * note: it can only be used to restore to newly created segment, which require the resource to
 * be manually removed, then re-created by service operator; 2. if there is available backup
 * (either from periodical or ad-hoc backup), then, restore the resource to a new cluster which
 * provide the ability of data migration (note: client need request to the new cluster after migration).
 *
 * TODO (Kangnan) rescue writes loss due to: 1. writes since the backup; 2. writes during restore
 * due to DB close.
 *
 * use case 1:
 *            - - - - - - - - - - - - - -  - - - - - - - - - - - - - -
 *           |   s3   (backupVersion v1)   (backupVersion v2)   ...   |
 *            - - - - - - - - - - - - - -  - - - - - - - - - - - - - -
 *                 ^                                 |
 *                 | (periodical backup)             |  (restore from v2)
 *                 |                                 v
 *           - - - - - - - - - - - - - -  - - - - - - - - - - - - - -
 *          | source/dest cluster: [shard1->v2], [shard2->v2],   ...  |
 *           - - - - - - - - - - - - - -  - - - - - - - - - - - - - -
 *
 * use case 2:
 *            - - - - - - - - - - - - - -  - - - - - - - - - - - - - -
 *           |   s3   (backupVersion v1)   (backupVersion v2)   ...   |
 *            - - - - - - - - - - - - - -  - - - - - - - - - - - - - -
 *            ^                                         |
 *            | (ad-hoc or periodical backup)           |  (restore from v2)
 *            |                                         v
 *      - - - - - - - - -       - - - -  - - - - - - - - - - - - - - - - - - -
 *     | source cluster  |     | dest cluster: [shard1:v2], [shard2:v2],   ... |
 *      - - - - - - - - -       - - - -  - - - - - - - - - - - - - - - - - - -
 *
 * The Task will be running as targeted job similar to backupTask, which means the db resource
 * must exist first before the restoreTask to pinpoint the targetPartition and restore it.
 *
 * Since {@link RestoreTask} will only be used for {@link com.pinterest.rocksplicator.MasterSlaveStateModelFactory.MasterSlaveStateModel},
 * and restore task will be configured (ie. set "TargetPartitionStates": "Master" in {@link JobConfig})
 * to target 1 and only 1 replica in Master state (compared to {@link BackupTask} which target
 * 1 and only 1 replica either in Master or Slave with "TargetPartitionStates": "Master, Slave").
 * If replica factor (RF) > 1, then {@link RestoreTask} is also responsible to initiate restore
 * to all other replicas in state model: Slave/Offline/Error.
 *
 * TODO: we can extend to support restoreTask to target replica in either Master or Slave in case
 * transient missing Master replica.
 *
 * Upon Task creation, configs inherited from job configs JobCommandConfigMap: STORE_PATH_PREFIX,
 * the cloud path prefix stored the backup; RESOURCE_VERSION: the timestamp the available backup
 * to restore from; "prefix/[version]/[dbName]" will be the full filesystem path to get db from
 * cloud; (optional) SRC_CLUSTER, default same as dest(task) cluster.
 */
public class RestoreTaskFactory implements TaskFactory {

  private static final Logger LOG = LoggerFactory.getLogger(RestoreTaskFactory.class);

  private final String cluster;
  private final int adminPort;
  private final boolean useS3Store;
  private final String s3Bucket;

  public RestoreTaskFactory(String cluster, int adminPort, boolean useS3Store, String s3Bucket) {
    this.cluster = cluster;
    this.adminPort = adminPort;
    this.useS3Store = useS3Store;
    this.s3Bucket = s3Bucket;
  }

  @Override
  public Task createNewTask(TaskCallbackContext context) {
    HelixAdmin admin = context.getManager().getClusterManagmentTool();

    TaskConfig taskConfig = context.getTaskConfig();
    JobConfig jobConfig = context.getJobConfig();
    String job = jobConfig.getJobId();

    String storePathPrefix = "";
    // in use case 2: sync latest data from src_cluster to dest_cluster
    String src_cluster = cluster;
    long resourceVersion = 0L;

    try {
      Map<String, String> jobCmdMap = jobConfig.getJobCommandConfigMap();
      if (jobCmdMap != null && !jobCmdMap.isEmpty()) {
        if (jobCmdMap.containsKey("STORE_PATH_PREFIX")) {
          storePathPrefix = jobCmdMap.get("STORE_PATH_PREFIX");
        }
        if (jobCmdMap.containsKey("SRC_CLUSTER")) {
          src_cluster = jobCmdMap.get("SRC_CLUSTER");
        }
        if (jobCmdMap.containsKey("RESOURCE_VERSION")) {
          resourceVersion = Long.parseLong(jobCmdMap.get("RESOURCE_VERSION"));
        }
      }
    } catch (NumberFormatException e) {
      LOG.error(
          String.format(
              "Failed to parse configs from job command config map, resourceVersion: %d",
              resourceVersion), e);
    }

    String targetPartition = taskConfig.getTargetPartition();

    LOG.error(
        String.format(
            "Create Task for cluster: %s, targetPartition: %s from job: %s to execute at "
                + "localhost, port: %d. {src_cluster: %s, resourceVersion: %d, "
                + "taskCreationTime: %d}",
            cluster, targetPartition, job, adminPort, src_cluster, resourceVersion,
            System.currentTimeMillis()));

    return getTask(admin, cluster, targetPartition, storePathPrefix, src_cluster, resourceVersion, job,
        adminPort, useS3Store, s3Bucket);
  }

  protected Task getTask(HelixAdmin admin, String cluster, String targetPartition, String storePathPrefix,
                         String src_cluster, long resourceVersion, String job, int port,
                         boolean useS3Store, String s3Bucket) {
    return new RestoreTask(admin, cluster, targetPartition, storePathPrefix, src_cluster,
        resourceVersion, job, port, useS3Store, s3Bucket);
  }
}
