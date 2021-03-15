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
import org.apache.helix.model.ExternalView;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * ingest sst files from cloud to a local DB (ingest ahead or behind). Currently, only support
 * ingest from S3 (could extend to support HDFS)
 */
public class IngestTask extends UserContentStore implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(IngestTask.class);

  private HelixAdmin admin;
  private final String taskCluster;
  private final String partitionName;
  private final String job;
  private final boolean ingest_behind;
  private final String host;
  private final int adminPort;
  private final int downloadLimitMbs;
  private final String partitionStorePath;
  private final String s3Bucket;
  private final TaskConfig taskConfig;

  public IngestTask(HelixAdmin admin, String taskCluster, String partitionName, String job,
                    boolean ingest_behind, String host, int adminPort, int downloadLimitMbs,
                    String s3Bucket,
                    String partitionStorePath, TaskConfig taskConfig) {
    this.admin = admin;
    this.taskCluster = taskCluster;
    this.partitionName = partitionName;
    this.job = job;
    this.ingest_behind = ingest_behind;
    this.host = host;
    this.adminPort = adminPort;
    this.downloadLimitMbs = downloadLimitMbs;
    this.s3Bucket = s3Bucket;
    this.partitionStorePath = partitionStorePath;
    this.taskConfig = taskConfig;
  }

  /**
   * Execute the task.
   *
   * @return A {@link TaskResult} object indicating the status of the task and any additional
   *         context information that can be interpreted by the specific {@link Task}
   *         implementation.
   */
  @Override
  public TaskResult run() {
    String dbName = Utils.getDbName(partitionName);
    if (partitionStorePath.isEmpty()) {
      String errMsg = String.format(
          "Cancel the task, partitionStorePath is not provided from job command config map. "
              + "taskConfig=%s",
          taskConfig.toString());
      LOG.error(errMsg);
      return new TaskResult(TaskResult.Status.CANCELED, errMsg);
    }

    try {
      String resourceName = dbName.substring(0, dbName.length() - 5);
      ExternalView view = admin.getResourceExternalView(taskCluster, resourceName);
      Map<String, String> stateMap = view.getStateMap(partitionName);
      List<String> errMsgs = new ArrayList<>();
      if (stateMap.size() != 0) {
        if (stateMap.containsValue("SLAVE") || stateMap.containsValue("MASTER")
            || stateMap.containsValue("FOLLOWER") || stateMap.containsValue("LEADER")) {
          int numReplica = stateMap.size();
          int numSuccIngestReplica = 0;
          for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
            String hostPort = instanceNameAndRole.getKey();
            String dbRole = instanceNameAndRole.getValue();
            String dbHost = hostPort.split("_")[0];
            int dbPort = Integer.parseInt(hostPort.split("_")[1]);

            LOG.error(String.format(
                "IngestTask run to ingest partition: %s from partitionStorePath: %s to instance: "
                    + "%s, "
                    + "role: %s. Other info {taskCluster: %s, job: %s, RpcFrom: %s_%d, "
                    + "taskConfig=%s}",
                partitionName, partitionStorePath, hostPort, dbRole, taskCluster, job, host,
                adminPort, taskConfig.toString()));

            try {
              executeIngest(dbHost, dbPort, dbName, ingest_behind, downloadLimitMbs, s3Bucket,
                  partitionStorePath);
              numSuccIngestReplica++;
            } catch (Exception e) {
              String errMsg =
                  String.format("Ingestion failed on %s. errMsg=%s.", host, e.getMessage());
              errMsgs.add(errMsg);
              LOG.error(
                  String.format("%s. stacktrack=%s", errMsg, Arrays.toString(e.getStackTrace())));
            }
          }
          if (numSuccIngestReplica != numReplica) {
            throw new RuntimeException(String.format(
                "Not all replica successfully execute ingestion, %s out of %s return success. "
                    + "errMsgs=[%s]",
                numSuccIngestReplica, numReplica, String.join(", ", errMsgs)));
          } else {
            LOG.info("Successfully ingest to all replicas of " + partitionName);
          }
        } else {
          throw new RuntimeException(
              "Skip ingest due to no Master/Slave or Leader/Follower replica found for partition: "
                  + partitionName);
        }
      } else {
        throw new RuntimeException(
            "Skip ingest due to empty stateMap got from resource externalView for partition: "
                + partitionName);
      }
    } catch (Exception e) {
      String errMsg = String.format("Task ingest failed. errMsg=%s. stacktrack=%s. taskConfig=%s.",
          e.getMessage(), Arrays.toString(e.getStackTrace()), taskConfig.toString());
      LOG.error(errMsg);
      return new TaskResult(TaskResult.Status.FAILED, errMsg);
    }

    return new TaskResult(TaskResult.Status.COMPLETED, "IngestTask is completed!");
  }

  protected void executeIngest(String host, int port, String dbName, boolean ingest_behind,
                               int downloadLimitMbs, String s3Bucket, String partitionStorePath)
      throws RuntimeException {
    try {
      Utils.ingestFromS3(
          host, port, dbName, ingest_behind, downloadLimitMbs, s3Bucket, partitionStorePath);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Signals the task to stop execution. The task implementation should carry out any clean up
   * actions that may be required and return from the {@link #run()} method.
   *
   * with default TaskStateModel, "cancel()" invoked by
   * {@link org.apache.helix.task.TaskRunner #cancel()} during state transitions:
   * running->stopped /task_aborted /dropped /init, and during
   * {@link org.apache.helix.task.TaskStateModel #reset()}
   */
  @Override
  public void cancel() {
    LOG.error(String.format("IngestTask cancelled. taskConfig=%s", taskConfig.toString()));
  }
}
