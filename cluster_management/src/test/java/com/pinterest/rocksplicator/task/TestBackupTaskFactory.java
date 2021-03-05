package com.pinterest.rocksplicator.task;

import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;


public class TestBackupTaskFactory extends TaskTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupTaskFactory.class);

  private static final String JOB_COMMAND = "DummyCommand";
  private static final int NUM_JOB = 2;
  private static final int NUM_TASK_PER_JOB = 2;
  private static final int NUM_TASK = NUM_JOB * NUM_TASK_PER_JOB;
  private Map<String, String> _jobCommandMap;

  private static final long fakeResourceVersion = 1234L;
  private static final String fakeS3Bucket = "pinterest-fake-bucket";

  private final CountDownLatch allTasksReady = new CountDownLatch(NUM_TASK);
  private final CountDownLatch adminReady = new CountDownLatch(1);


  @Override
  protected void startParticipant(String zkAddr, int i) {
    final String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
    Map<String, TaskFactory> taskFactoryReg = new HashMap();
    taskFactoryReg.put("Backup", new DummyBackupTaskFactory(CLUSTER_NAME,
        Integer.parseInt(instanceName.split("_")[1]), true, fakeS3Bucket));
    this._participants[i] = new MockParticipantManager(zkAddr, this.CLUSTER_NAME, instanceName);
    StateMachineEngine stateMachine = this._participants[i].getStateMachineEngine();
    stateMachine.registerStateModelFactory("Task",
        new TaskStateModelFactory(this._participants[i], taskFactoryReg));
    this._participants[i].syncStart();
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _jobCommandMap = new HashMap<>();
  }

  @BeforeMethod
  public void setUp(Method m) throws Exception {
    String workflowName = m.getName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // Create 2 jobs with 2 Backup Tasks each
    for (int i = 0; i < NUM_JOB; i++) {
      String jobName = "JOB" + i;

      _jobCommandMap.put("STORE_PATH_PREFIX", "/test_cloud");
      _jobCommandMap.put("BACKUP_LIMIT_MBS", String.valueOf(10));
      _jobCommandMap.put("RESOURCE_VERSION", String.valueOf(fakeResourceVersion + i));

      List<TaskConfig> taskConfigs = new ArrayList<>();
      for (int j = 0; j < NUM_TASK_PER_JOB; ++j) {
        Map<String, String> taskConfigMap = new HashMap<>();
        taskConfigMap.put("TASK_TARGET_PARTITION", "test_seg_" + j);
        taskConfigs.add(new TaskConfig("Backup", taskConfigMap));
      }

      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap);
      workflowBuilder.addJob(jobName, jobConfigBulider);
    }

    // Start the workflow and wait for all tasks started
    _driver.start(workflowBuilder.build());
    allTasksReady.await();

    adminReady.countDown();
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);
  }

  @AfterMethod
  public void cleanUp(Method m) throws Exception {
    _jobCommandMap.clear();
  }

  @Test
  public void testBackupTaskFactoryCreateNewTaskWithPassedConfigs() throws Exception {
    String workflowName = TestHelper.getTestMethodName();

    Assert.assertEquals(_driver.getWorkflowConfig(workflowName).getWorkflowId(), workflowName);

    for (int i = 0; i < NUM_JOB; i++) {
      String jobName = "JOB" + i;
      String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
      JobConfig jobConfig = _driver.getJobConfig(namespacedJobName);

      Assert.assertEquals(jobConfig.getCommand(), JOB_COMMAND);

      Set<String> taskTargetParts = new HashSet<>();
      int taskPartitionId = 0;
      for (TaskConfig taskConfig : _driver.getJobConfig(namespacedJobName).getTaskConfigMap()
          .values()) {
        Assert.assertEquals(taskConfig.getCommand(), "Backup");
        taskTargetParts.add(taskConfig.getTargetPartition());

        Assert.assertEquals(jobConfig.getJobCommandConfigMap().get("STORE_PATH_PREFIX"),
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("storePathPrefix"));

        Assert.assertEquals(jobConfig.getJobCommandConfigMap().get("BACKUP_LIMIT_MBS"),
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("backupLimitMbs"));

        Assert.assertEquals(jobConfig.getJobCommandConfigMap().get("RESOURCE_VERSION"),
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("resourceVersion"));

        Assert.assertEquals(
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("useS3Store"), "true");

        Assert.assertEquals(
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("s3Bucket"),
            fakeS3Bucket);

        taskPartitionId++;
      }

      Assert
          .assertEquals(taskTargetParts, new HashSet<>(Arrays.asList("test_seg_0", "test_seg_1")));
    }
  }

  @Test
  public void testResVersionDifferAmongTasksFromDiffJobs() throws Exception {
    String workflowName = TestHelper.getTestMethodName();

    Set<String> resVersions = new HashSet<>();
    for (int i = 0; i < NUM_JOB; i++) {
      String job = "JOB" + i;
      String jobResVersion = _driver.getJobUserContentMap(workflowName, job).get("resourceVersion");

      Assert.assertTrue(resVersions.add(jobResVersion));
    }
    Assert.assertEquals(resVersions.size(), 2);
  }

  @Test
  public void testResourceVersionSameAmongTasksFromSameJob() throws Exception {
    String workflowName = TestHelper.getTestMethodName();

    for (int i = 0; i < NUM_JOB; ++i) {
      String jobName = "JOB" + 0;
      Set<String> resourceVersions = new HashSet<>();
      for (int j = 0; j < NUM_TASK_PER_JOB; j++) {
        resourceVersions.add(_driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(j))
            .get("resourceVersion"));
      }
      Assert.assertEquals(resourceVersions.size(), 1);
    }
  }

  //*****************************************************
  // dummy TaskFactory, Task to limit testing scope
  //
  // Varied behavior based on specific testing class:
  // - populate Task level userStore
  //****************************************************/

  private class DummyBackupTaskFactory extends BackupTaskFactory {

    public DummyBackupTaskFactory(String cluster, int adminPort, boolean useS3Store,
                                  String s3Bucket) {
      super(cluster, adminPort, useS3Store, s3Bucket);
    }

    @Override
    protected Task getTask(String cluster, String targetPartition, int backupLimitMbs,
                           String storePathPrefix, long resourceVersion, String job, int port,
                           boolean useS3Store, String s3Bucket, boolean shareFilesWithChecksum) {
      return new DummyBackupTask(cluster, targetPartition, backupLimitMbs, storePathPrefix,
          resourceVersion, job, port, useS3Store, s3Bucket, shareFilesWithChecksum);
    }

  }

  private class DummyBackupTask extends BackupTask {

    public DummyBackupTask(String taskCluster, String partitionName, int backupLimitMbs,
                           String storePathPrefix, long resourceVersion, String job, int adminPort,
                           boolean useS3Store, String s3Bucket, boolean shareFilesWithChecksum) {
      super(taskCluster, partitionName, backupLimitMbs, storePathPrefix, resourceVersion, job,
          adminPort, useS3Store, s3Bucket, shareFilesWithChecksum);
    }

    @Override
    public TaskResult run() {
      allTasksReady.countDown();
      try {
        adminReady.await();
      } catch (Exception e) {
        return new TaskResult(TaskResult.Status.FATAL_FAILED, e.getMessage());
      }

      // store task info into Task level userStore to compare with BackupTaskfactory
      try {
        long resVersion = readPrivateSuperClassLongField("resourceVersion");
        int backupLimitMbs = readPrivateSuperClassIntField("backupLimitMbs");
        String storePathPrefix = readPrivateSuperClassStringField("storePathPrefix");
        putUserContent("resourceVersion", String.valueOf(resVersion), Scope.JOB);
        putUserContent("resourceVersion", String.valueOf(resVersion), Scope.TASK);
        putUserContent("backupLimitMbs", String.valueOf(backupLimitMbs), Scope.TASK);
        putUserContent("storePathPrefix", storePathPrefix, Scope.TASK);
      } catch (Exception e) {
        LOG.error(
            "Failed to read super class's private filed or fail to pur userStore" + e.getMessage());
      }

      try {
        super.run();
      } catch (Exception e) {
        LOG.error("Failed to execute BackupTask run" + e.getMessage());
      }

      return new TaskResult(TaskResult.Status.COMPLETED, "");
    }

    @Override
    protected void executeBackup(String host, int port, String dbName, String storePath,
                                 int backupLimitMbs, boolean useS3Store, String s3Bucket,
                                 boolean shareFilesWithChecksum)
        throws RuntimeException {
      try {
        boolean useS3 = readPrivateSuperClassBooleanField("useS3Store");
        String bucket = readPrivateSuperClassStringField("s3Bucket");
        putUserContent("useS3Store", String.valueOf(useS3), Scope.TASK);
        putUserContent("s3Bucket", bucket, Scope.TASK);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void cancel() {
    }

    // helper function for testing
    // java refection: http://tutorials.jenkov.com/java-reflection/private-fields-and-methods.html

    private long readPrivateSuperClassLongField(String fieldName) throws Exception {
      Class<?> clazz = getClass().getSuperclass();
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.getLong(this);
    }

    private int readPrivateSuperClassIntField(String fieldName) throws Exception {
      Class<?> clazz = getClass().getSuperclass();
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.getInt(this);
    }

    private String readPrivateSuperClassStringField(String fieldName) throws Exception {
      Class<?> clazz = getClass().getSuperclass();
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return (String) field.get(this);
    }

    private boolean readPrivateSuperClassBooleanField(String fieldName) throws Exception {
      Class<?> clazz = getClass().getSuperclass();
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return (Boolean) field.get(this);
    }
  }
}
