package com.pinterest.rocksplicator.task;

import com.pinterest.rocksplicator.TaskUtils;
import com.pinterest.rocksplicator.Utils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.Locker;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DedupTask extends UserContentStore implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(DedupTask.class);

  private final String taskCluster;
  private final String resourceCluster;
  private final String resourceName;
  private final String partitionName;
  private long resourceVersion;
  private final long jobVersion;
  private String job;
  private final String host;
  private final int adminPort;
  private CuratorFramework zkClient;
  private InterProcessMutex jobVersionMutex;

  public DedupTask(String taskCluster, String resourceCluster, String resourceName,
                   String partitionName, long resourceVersion,
                   long jobVersion, String job, String host, int adminPort,
                   CuratorFramework zkClient) {
    this.taskCluster = taskCluster;
    this.resourceCluster = resourceCluster;
    this.resourceName = resourceName;
    this.partitionName = partitionName;
    this.resourceVersion = resourceVersion;
    this.jobVersion = jobVersion;
    this.job = job;
    this.host = host;
    this.adminPort = adminPort;
    this.zkClient = zkClient;
    this.jobVersionMutex = new InterProcessMutex(zkClient,
        TaskUtils.getSegmentLockPath(taskCluster, resourceCluster, resourceName, jobVersion));
  }

  /**
   * Execute the task.
   * @return A {@link TaskResult} object indicating the status of the task and any additional
   *         context information that can be interpreted by the specific {@link Task}
   *         implementation.
   */
  @Override
  public TaskResult run() {

    // check task created with resource cluster
    if (resourceCluster.isEmpty()) {
      String errMsg = "resource cluster is not empty, not provided from jobConfig";
      LOG.error(errMsg);
      return new TaskResult(TaskResult.Status.CANCELED, errMsg);
    }

    // check task created with resource name and match prefix of target partition
    String dbName = Utils.getDbName(partitionName);
    String segmentName = partitionName.substring(0, partitionName.lastIndexOf('_'));
    if (!resourceName.equals(segmentName)) {
      String errMsg = String.format("RESOURCE {%s} provided from jobConfig is different from "
          + "targetPartition {%s}'s prefix", resourceName, partitionName);
      LOG.error(errMsg);
      return new TaskResult(TaskResult.Status.CANCELED, errMsg);
    }

    // check task created with a valid resource version
    if (resourceVersion == -1) {
      // no valid version (e.g. no backup version ready, failed to retrieve state of resource)
      return new TaskResult(TaskResult.Status.CANCELED, "No valid resource version found");
    }

    // unify resource version to among tasks in the same job
    try (Locker locker = new Locker(jobVersionMutex)) {
      String curtDedupResourceVersion = getUserContent("resourceVersion", Scope.JOB);
      if (curtDedupResourceVersion == null || curtDedupResourceVersion.isEmpty()) {
        putUserContent("resourceVersion", String.valueOf(resourceVersion), Scope.JOB);
        putUserContent("jobVersion", String.valueOf(jobVersion), Scope.JOB);
        putUserContent("resourceCluster", resourceCluster, Scope.JOB);
      } else {
        try {
          resourceVersion = Long.parseLong(curtDedupResourceVersion);
        } catch (NumberFormatException e) {
          LOG.error("Failed to parse resource version from userStore", e);
        }
      }
    } catch (Exception e) {
      String errMsg =
          String.format("Failed to release the mutex for {resource: %s, jobVersion: %d}, %s",
              resourceName,
              jobVersion,
              e.getMessage());
      LOG.error(errMsg);
      return new TaskResult(TaskResult.Status.FAILED, errMsg);
    }

    try {

      // only execute dedup when: 1. backup has completed version; 2. the version is not
      // successfully exported yet (e.g. NOT_STARTED, FAILED). if export is in
      // (IN_PROGRESS, STOPPING, PAUSED, COMPLETED) state, skip curt export job.
      // there could only have 1 job version executed for same resource version at one time
      TaskUtils.BackupStatus
          backupStatus =
          getBackupStatus(resourceCluster, resourceName, resourceVersion);

      if (backupStatus == TaskUtils.BackupStatus.COMPLETED) {

        TaskUtils.ExportStatus exportStatus =
            getExportStatus(taskCluster, segmentName, resourceVersion, jobVersion);

        if (exportStatus != TaskUtils.ExportStatus.COMPLETED) {

          String src_hdfsPath =
              String.format("/backup_test/rocksplicator/%s/%s/%s/%s", resourceCluster, segmentName,
                  String.valueOf(resourceVersion), dbName);

          String dest_hdfsPath =
              String.format("/backup_test/compacted/rocksplicator/%s/%s/%s/%s", resourceCluster,
                  segmentName, String.valueOf(resourceVersion), dbName);

          LOG.error(
              String.format(
                  "DedupTask run to deup partition: %s from hdfsPath: %s at host: %s, port: %d. "
                      + "Other info {cluster: %s, job: %s, resourceVersion: %d}", partitionName,
                  src_hdfsPath, host, adminPort, resourceCluster, job, resourceVersion));

          executeDedup(dbName, adminPort, src_hdfsPath, dest_hdfsPath);

          LOG.error("DedupTask completed, with: success");
          return new TaskResult(TaskResult.Status.COMPLETED, "DedupTask is completed!");
        } else {
          String errLog = String.format(
              "Cluster: %s, db: %s, version: %d has already been exported, skip the dedup task",
              resourceCluster,
              dbName,
              resourceVersion);
          LOG.error(errLog);
          return new TaskResult(TaskResult.Status.CANCELED, errLog);
        }
      } else {
        String errLog =
            String.format("Backup data not ready for cluster: %s, segment: %s, resourceVersion: %d",
                resourceCluster,
                segmentName, resourceVersion);
        LOG.error(errLog);
        return new TaskResult(TaskResult.Status.CANCELED, errLog);
      }
    } catch (Exception e) {
      LOG.error("Task dedup failed", e);
      return new TaskResult(TaskResult.Status.FAILED, "DedupTask failed");
    }

  }

  protected TaskUtils.BackupStatus getBackupStatus(String cluster, String segment,
                                                   long resVersion) throws RuntimeException {
    try {
      return TaskUtils.getBackupStatus(cluster, segment, resVersion);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected TaskUtils.ExportStatus getExportStatus(String taskCluster, String segment,
                                                   long resourceVersion, long jobVersion)
      throws RuntimeException {
    try {
      return TaskUtils.getExportStatus(taskCluster, segment, resourceVersion, jobVersion);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void executeDedup(String dbName, int adminPort, String src_hdfsPath,
                              String dest_hdfsPath) throws RuntimeException {
    try {
      Utils.addDB(dbName, adminPort);
      Utils.closeDB(dbName, adminPort);
      Utils.restoreLocalDB(adminPort, dbName, src_hdfsPath, "127.0.0.1", adminPort);
      LOG.error("restoreDB is done, begin compactDB");

      Utils.compactDB(adminPort, dbName);
      LOG.error("compactDB is done");

      // TODO(kangnan) check db integration before backup

      Utils.backupDB("127.0.0.1", adminPort, dbName, dest_hdfsPath);
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