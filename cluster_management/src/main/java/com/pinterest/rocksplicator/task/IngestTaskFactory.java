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

import org.apache.helix.HelixAdmin;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * An implementation of {@link TaskFactory} that create Tasks upon ingest-job launching.
 *
 * This class takes in {@param adminPort}, {@param cluster}, from
 * {@link com.pinterest.rocksplicator.Participant} to create tasks to backup local db to cloud.
 **
 * Upon Task creation, configs inherited from job configs JobCommandConfigMap: INGEST_BEHIND,
 * whether instruct to ingest data ahead or behind existing data; RESOURCE_STORE_PATH, the s3 path
 * for the whole version of the segment data, e.g. "<cluster>/<seg>/1596739476/", which will further
 * append the suffix for the specific partition to buidl the <s3_path> for the partition ingestion
 * request, e.g. "<cluster>/<seg>/1596739476/part-%05d-"; (optional) DOWNLOAD_LIMIT_MBS, specify
 * download limit by mbs.
 */
public class IngestTaskFactory implements TaskFactory {

  private static final Logger LOG = LoggerFactory.getLogger(IngestTaskFactory.class);

  private final String cluster;
  private final String host;
  private final int adminPort;
  private String s3Bucket;

  private static final int DEFAULT_DOWNLOAD_LIMIT_MBS = 64;

  public IngestTaskFactory(String cluster, String host, int adminPort, String s3Bucket) {
    this.cluster = cluster;
    this.host = host;
    this.adminPort = adminPort;
    this.s3Bucket = s3Bucket;
  }

  /**
   * Returns a {@link Task} instance.
   *
   * @param context Contextual information for the task, including task and job configurations
   */
  @Override
  public Task createNewTask(TaskCallbackContext context) {
    HelixAdmin admin = context.getManager().getClusterManagmentTool();

    TaskConfig taskConfig = context.getTaskConfig();
    JobConfig jobConfig = context.getJobConfig();
    String job = jobConfig.getJobId();

    String targetPartition = taskConfig.getTargetPartition();
    boolean ingest_behind = true;
    String resourceStorePath = null;
    int downloadLimitMbs = DEFAULT_DOWNLOAD_LIMIT_MBS;

    try {
      Map<String, String> jobCmdMap = jobConfig.getJobCommandConfigMap();
      if (jobCmdMap != null && !jobCmdMap.isEmpty()) {
        if (jobCmdMap.containsKey("INGEST_BEHIND")) {
          ingest_behind = Boolean.parseBoolean(jobCmdMap.get("INGEST_BEHIND"));
        }
        if (jobCmdMap.containsKey("S3_BUCKET")) {
          // by default, use the S3Bucket of the FLAGS_s3_bucket_backup
          s3Bucket = jobCmdMap.get("S3_BUCKET");
        }
        if (jobCmdMap.containsKey("RESOURCE_STORE_PATH")) {
          resourceStorePath = jobCmdMap.get("RESOURCE_STORE_PATH");
        }
        if (jobCmdMap.containsKey("DOWNLOAD_LIMIT_MBS")) {
          downloadLimitMbs = Integer.parseInt(jobCmdMap.get("DOWNLOAD_LIMIT_MBS"));
        }
      }

      if (targetPartition == null || resourceStorePath == null) {
        throw new RuntimeException(String.format(
            "Failed to get required configs from job/task config, targetPartition: %s, "
                + "resourceStorePath: %s",
            targetPartition, resourceStorePath));
      }
    } catch (NumberFormatException e) {
      LOG.error(String.format(
          "Failed to parse configs from job/task configs, use defaults downloadLimitMbs: %d. "
              + "errMsg=%s. stacktrace=%s",
          DEFAULT_DOWNLOAD_LIMIT_MBS, e.getMessage(), Arrays.toString(e.getStackTrace())));
    } catch (Exception e) {
      LOG.error(String.format("Failed to createNewTask. taskConfig=%s. errMsg=%s. stacktrace=%s",
          taskConfig.toString(), e.getMessage(), Arrays.toString(e.getStackTrace())));
      return null;
    }

    LOG.error(
        String.format("Create Task for cluster: %s, targetPartition: %s from job: %s to execute at "
                + "host: %s, port: %d. {taskCreationTime: %d, taskConfig: %s}",
            cluster, targetPartition, job, host, adminPort, System.currentTimeMillis(),
            taskConfig.toString()));

    return getTask(admin, cluster, targetPartition, job, ingest_behind, host, adminPort,
        downloadLimitMbs, s3Bucket, resourceStorePath + Utils.getS3PartPrefix(targetPartition),
        taskConfig);
  }

  protected Task getTask(HelixAdmin admin, String cluster, String targetPartition, String job,
                         boolean ingest_behind, String host, int port, int downloadLimitMbs,
                         String s3Bucket,
                         String partitionStorePath, TaskConfig taskConfig) {
    return new IngestTask(admin, cluster, targetPartition, job, ingest_behind, host, port,
        downloadLimitMbs, s3Bucket, partitionStorePath, taskConfig);
  }
}
