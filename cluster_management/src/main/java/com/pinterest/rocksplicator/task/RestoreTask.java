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

import com.pinterest.rocksdb_admin.thrift.CheckDBResponse;
import com.pinterest.rocksplicator.Utils;

import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

public class RestoreTask extends UserContentStore implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(RestoreTask.class);

  private final String taskCluster;
  private final String partitionName;
  private final String storePathPrefix;
  private final String srcCluster;
  private final long resourceVersion;
  private final String job;
  private final int adminPort;
  private final boolean useS3Store;
  private final String s3Bucket;
  private final TaskConfig taskConfig;
  private HelixAdmin admin;


  public RestoreTask(HelixAdmin admin, String taskCluster, String partitionName,
                     String storePathPrefix, String srcCluster, long resourceVersion, String job,
                     int adminPort, boolean useS3Store, String s3Bucket, TaskConfig taskConfig) {
    this.admin = admin;
    this.taskCluster = taskCluster;
    this.partitionName = partitionName;
    this.storePathPrefix = storePathPrefix;
    this.srcCluster = srcCluster;
    this.resourceVersion = resourceVersion;
    this.job = job;
    this.adminPort = adminPort;
    this.useS3Store = useS3Store;
    this.s3Bucket = s3Bucket;
    this.taskConfig = taskConfig;
  }

  @Override
  public TaskResult run() {
    String dbName = Utils.getDbName(partitionName);
    if (storePathPrefix.isEmpty() || resourceVersion == 0L) {
      String errMsg =
          "Cancel the task, storePathPrefix/resourceVersion is not provided from job command "
              + "config map";
      LOG.error(errMsg);
      return new TaskResult(TaskResult.Status.CANCELED, errMsg);
    }

    try {
      String storePath =
          String.format("%s/%s/%s", storePathPrefix, String.valueOf(resourceVersion), dbName);

      // restore same data to other replicas of this parition
      Map<String, String> stateMap = getPartitionStateMap(admin, taskCluster, partitionName);
      if (stateMap.size() != 0) {
        if (stateMap.containsValue("SLAVE") || stateMap.containsValue("MASTER") ||
            stateMap.containsValue("FOLLOWER") || stateMap.containsValue("LEADER")) {
          for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
            String hostPort = instanceNameAndRole.getKey();
            String role = instanceNameAndRole.getValue();
            String host = hostPort.split("_")[0];
            int port = Integer.parseInt(hostPort.split("_")[1]);

            LOG.error(
                String.format(
                    "RestoreTask run to restore partition: %s from storePath: %s to host: %s, "
                        + "role: %s. Other info {src_cluster: %s, taskCluster: %s, job: %s, "
                        + "version: %d, taskConfig: %s}", partitionName, storePath, host, role,
                    srcCluster, taskCluster, job, resourceVersion, taskConfig.toString()));

            executeRestore(host, port, dbName, storePath, useS3Store, s3Bucket);
          }
        } else {
          throw new RuntimeException(
              "Skip restore due to no Master or Slave replica found for partition: "
                  + partitionName);
        }
      } else {
        throw new RuntimeException(
            "Skip restore due to empty stateMap got from resource externalView for partition: "
                + partitionName);
      }

      return new TaskResult(TaskResult.Status.COMPLETED, "RestoreTask is completed!");
    } catch (Exception e) {
      String errMsg =
          String
              .format("Task restore failed. errMsg=%s. stacktrace=%s. taskConfig=%s",
                  e.getMessage(),
                  Arrays.toString(e.getStackTrace()), taskConfig.toString());
      LOG.error(errMsg);
      return new TaskResult(TaskResult.Status.FAILED, errMsg);
    }
  }

  protected void executeRestore(String host, int adminPort, String dbName, String storePath,
                                boolean useS3Store, String s3Bucket)
      throws RuntimeException {
    try {
      CheckDBResponse
          checkDBResponse =
          Utils.checkRemoteOrLocalDB(host, adminPort, dbName, false, null, null);
      boolean isLeader = checkDBResponse.isIs_master();
      LOG.error(String.format("Execute restore %s to %s, whose dbRole is %s", dbName, host,
          isLeader ? "LEADER" : "FOLLOWER"));

      Utils.closeRemoteOrLocalDB(host, adminPort, dbName);
      if (useS3Store) {
        Utils.restoreRemoteOrLocalDBFromS3(host, adminPort, dbName, s3Bucket, storePath, host,
            adminPort);
      } else {
        Utils.restoreRemoteOrLocalDB(host, adminPort, dbName, storePath, host, adminPort);
      }

      Map<String, String>
          stateMap =
          getPartitionStateMap(admin, taskCluster, Utils.getPartitionName(dbName));
      if (stateMap.containsValue("MASTER") || stateMap.containsValue("LEADER")) {
        for (Map.Entry<String, String> instanceNameAndRole : stateMap.entrySet()) {
          String role = instanceNameAndRole.getValue();
          if (role.equals("MASTER") || role.equals("LEADER")) {
            String hostPort = instanceNameAndRole.getKey();
            String leaderHost = hostPort.split("_")[0];
            int leaderPort = Integer.parseInt(hostPort.split("_")[1]);
            if (isLeader && !leaderHost.equals(host)) {
              LOG.error("Partition leader has switched from %s to %s based on externalView", host,
                  leaderHost);
            }
            Utils.changeDBRoleAndUpStream(leaderHost, leaderPort, dbName, "LEADER", leaderHost,
                leaderPort);
          }
        }
      } else {
        LOG.error("No leader role obtained from partition's externalView: %s", stateMap.toString());
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, String> getPartitionStateMap(HelixAdmin admin, String cluster,
                                                   String partitionName) {
    String resourceName = partitionName.substring(0, partitionName.lastIndexOf('_'));
    ExternalView view = admin.getResourceExternalView(cluster, resourceName);
    return view.getStateMap(partitionName);
  }

  @Override
  public void cancel() {
    LOG.error(String.format("RestoreTask cancelled. taskConfig=%s", taskConfig.toString()));
  }
}
