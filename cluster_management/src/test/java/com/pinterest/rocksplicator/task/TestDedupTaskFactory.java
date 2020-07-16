package com.pinterest.rocksplicator.task;

import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;

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


public class TestDedupTaskFactory extends TaskTestBase {

  private static final String JOB_COMMAND = "DummyCommand";
  private static final int NUM_JOB = 2;
  private static final int NUM_TASK_PER_JOB = 2;
  private static final int NUM_TASK = NUM_JOB * NUM_TASK_PER_JOB;
  private Map<String, String> _jobCommandMap;

  private final String fakeSrcStorePathPrefix = "/source/store";
  private final long fakeResourceVersion = 123456789L;
  private final String fakeDestStorePathPrefix = "/dest/store";

  private final CountDownLatch allTasksReady = new CountDownLatch(NUM_TASK);
  private final CountDownLatch adminReady = new CountDownLatch(1);

  @Override
  protected void startParticipant(String zkAddr, int i) {
    final String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    taskFactoryReg.put("Dedup",
        new DummyDedupTaskFactory(CLUSTER_NAME, Integer.parseInt(instanceName.split("_")[1]),
            false, ""));
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

    // Create 2 jobs with 2 Dedup Tasks each
    for (int i = 0; i < NUM_JOB; i++) {
      String jobName = "JOB" + i;

      _jobCommandMap.put("SRC_STORE_PATH_PREFIX", fakeSrcStorePathPrefix);
      _jobCommandMap.put("RESOURCE_VERSION", String.valueOf(fakeResourceVersion + i));
      _jobCommandMap.put("DEST_STORE_PATH_PREFIX", fakeDestStorePathPrefix);

      List<TaskConfig> taskConfigs = new ArrayList<>();
      for (int j = 0; j < NUM_TASK_PER_JOB; ++j) {
        Map<String, String> taskConfigMap = new HashMap<>();
        taskConfigMap.put("TASK_TARGET_PARTITION", "test_seg_" + j);
        taskConfigs.add(new TaskConfig("Dedup", taskConfigMap));
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
  public void testDedupTaskFactoryCreateNewTaskWithSpecifiedConfigs() throws Exception {
    String workflowName = TestHelper.getTestMethodName();

    System.out.println(workflowName);
    // verify Task Command is passed in: Dedup
    for (int i = 0; i < NUM_JOB; i++) {
      String jobName = "JOB" + i;
      String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
      JobConfig jobConfig = _driver.getJobConfig(namespacedJobName);

      Set<String> taskTargetParts = new HashSet<>();
      int taskPartitionId = 0;
      for (TaskConfig taskConfig : _driver.getJobConfig(namespacedJobName).getTaskConfigMap()
          .values()) {
        Assert.assertEquals(taskConfig.getCommand(), "Dedup");
        taskTargetParts.add(taskConfig.getTargetPartition());

        Assert.assertEquals(jobConfig.getJobCommandConfigMap().get("SRC_STORE_PATH_PREFIX"),
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("srcStorePathPrefix"));

        Assert.assertEquals(jobConfig.getJobCommandConfigMap().get("RESOURCE_VERSION"),
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("resourceVersion"));

        Assert.assertEquals(jobConfig.getJobCommandConfigMap().get("DEST_STORE_PATH_PREFIX"),
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("destStorePathPrefix"));
      }

      Assert
          .assertEquals(taskTargetParts, new HashSet<>(Arrays.asList("test_seg_0", "test_seg_1")));
      taskPartitionId++;
    }

    _jobCommandMap.clear();
  }


  protected class DummyDedupTaskFactory extends DedupTaskFactory {

    public DummyDedupTaskFactory(String cluster, int adminPort, boolean useS3Store,
                                 String s3Bucket) {
      super(cluster, adminPort, useS3Store, s3Bucket);
    }

    @Override
    protected DedupTask getTask(String srcStorePathPrefix, long resourceVersion,
                                String partitionName, String cluster, String job, int port,
                                String destStorePathPrefix, boolean useS3Store, String s3Bucket,
                                int backupLimitMbs, boolean shareFilesWithChecksum) {
      return new DummyDedupTask(srcStorePathPrefix, resourceVersion, partitionName, cluster, job, port,
          destStorePathPrefix, useS3Store, s3Bucket, backupLimitMbs, shareFilesWithChecksum);
    }

  }

  protected class DummyDedupTask extends DedupTask {

    public DummyDedupTask(String srcStorePathPrefix, long resourceVersion,
                          String partitionName, String taskCluster, String job, int adminPort,
                          String destStorePathPrefix, boolean useS3Store, String s3Bucket,
                          int backupLimitMbs, boolean shareFilesWithChecksum) {
      super(srcStorePathPrefix, resourceVersion, partitionName, taskCluster, job, adminPort,
          destStorePathPrefix, useS3Store, s3Bucket, backupLimitMbs, shareFilesWithChecksum);
    }

    @Override
    public TaskResult run() {
      allTasksReady.countDown();
      try {
        adminReady.await();
      } catch (Exception e) {
        return new TaskResult(TaskResult.Status.FATAL_FAILED, e.getMessage());
      }

      try {
        String srcStorePathPrefix = readPrivateSuperClassStringField("srcStorePathPrefix");
        long resVersion = readPrivateSuperClassLongField("resourceVersion");
        String destStorePathPrefix = readPrivateSuperClassStringField("destStorePathPrefix");
        putUserContent("srcStorePathPrefix", srcStorePathPrefix, Scope.TASK);
        putUserContent("resourceVersion", String.valueOf(resVersion), Scope.TASK);
        putUserContent("destStorePathPrefix", destStorePathPrefix, Scope.TASK);
      } catch (Exception e) {
        System.out.println(e.getCause());
      }

      return new TaskResult(TaskResult.Status.COMPLETED, "");
    }

    @Override
    public void cancel() {
    }

    // helper function for testing
    // java refection: http://tutorials.jenkov.com/java-reflection/private-fields-and-methods.html
    private String readPrivateSuperClassStringField(String fieldName) throws Exception {
      Class<?> clazz = getClass().getSuperclass();
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return (String) field.get(this);
    }

    private long readPrivateSuperClassLongField(String fieldName) throws Exception {
      Class<?> clazz = getClass().getSuperclass();
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.getLong(this);
    }
  }
}