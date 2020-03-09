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

public class BackupTask extends UserContentStore implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(BackupTask.class);

  private final String taskCluster;
  private final String partitionName;
  private final int backupLimitMbs;
  private long resourceVersion;
  private final String job;
  private final String host;
  private final int adminPort;
  private CuratorFramework zkClient;
  private InterProcessMutex jobVersionMutext;

  public BackupTask(String taskCluster, String resourceName, String partitionName,
                    int backupLimitMbs, long jobVersion, String job,
                    String host, int adminPort,
                    CuratorFramework zkClient) {
    this.taskCluster = taskCluster;
    this.partitionName = partitionName;
    this.backupLimitMbs = backupLimitMbs;
    this.resourceVersion = jobVersion;
    this.job = job;
    this.host = host;
    this.adminPort = adminPort;
    this.zkClient = zkClient;
    this.jobVersionMutext = new InterProcessMutex(zkClient,
        TaskUtils.getSegmentLockPath(taskCluster, taskCluster, resourceName, jobVersion));
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
    String segmentName = partitionName.substring(0, partitionName.lastIndexOf("_"));

    // backup does not need inter-process lock, since jobVersion equal resourceVersion and
    // jobVersion is already unified at jobConfig level. put into userStore for state map generation
    try (Locker locker = new Locker(jobVersionMutext)) {
      String curtBackupVersion = getUserContent("resourceVersion", UserContentStore.Scope.JOB);
      if (curtBackupVersion == null || curtBackupVersion.isEmpty()) {
        putUserContent("resourceVersion", String.valueOf(resourceVersion),
            UserContentStore.Scope.JOB);
        // for backup, jobVersion is resourceVersion; taskCluster is resourceCluster
        putUserContent("jobVersion", String.valueOf(resourceVersion), Scope.JOB);
        putUserContent("resourceCluster", taskCluster, UserContentStore.Scope.JOB);
      } else {
        if (!curtBackupVersion.equals(resourceVersion)) {
          LOG.error(String.format(
              "Current backup resourceVersion {%s} stored at Job level userStore differ from "
                  + "Task's resourceVersion {%d}", curtBackupVersion, resourceVersion));
        }
        try {
          resourceVersion = Long.parseLong(curtBackupVersion);
        } catch (NumberFormatException e) {
          LOG.error("Failed to parse resource version from userStore", e);
        }
      }
    } catch (Exception e) {
      String errMsg =
          String.format("Failed to release the mutex for {resource: %s, jobVersion: %d}, %s",
              segmentName,
              resourceVersion,
              e.getMessage());
      LOG.error(errMsg);
      return new TaskResult(TaskResult.Status.FAILED, errMsg);
    }

    try {

      String hdfsPath =
          String.format("/backup_test/rocksplicator/%s/%s/%s/%s", taskCluster, segmentName,
              String.valueOf(resourceVersion),
              dbName);

      LOG.error(String.format(
          "BackupTask run to backup partition: %s to hdfsPath: %s at host: %s, port: %d. Other info"
              + " {taskCluster: %s, job: %s, version: %d}", partitionName, hdfsPath, host,
          adminPort, taskCluster, job, resourceVersion));

      executeBackup("127.0.0.1", 9090, dbName, hdfsPath, backupLimitMbs);

      return new TaskResult(TaskResult.Status.COMPLETED, "BackupTask is completed!");
    } catch (Exception e) {
      LOG.error("Task backup failed");
      return new TaskResult(TaskResult.Status.FAILED, "BackupTask failed");
    }

  }

  protected void executeBackup(String host, int port, String dbName, String hdfsPath,
                               int backupLimitMbs) throws RuntimeException {
    try {
      Utils.backupDBWithLimit(host, port, dbName, hdfsPath, backupLimitMbs);
    } catch (Exception e){
      LOG.error("Failed to execute backup", e);
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
    LOG.error("BackupTask cancelled");
  }
}