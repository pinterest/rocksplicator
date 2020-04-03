package com.pinterest.rocksplicator.task;

import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.tools.ClusterSetup;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.Assert;

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

  private static final String JOB_COMMAND = "DummyCommand";
  private static final int NUM_JOB = 2;
  private static final int NUM_TASK_PER_JOB = 2;
  private static final int NUM_TASK = NUM_JOB * NUM_TASK_PER_JOB;
  private Map<String, String> _jobCommandMap;

  private static final long fakeResourceVersion = 1234L;

  private final CountDownLatch allTasksReady = new CountDownLatch(NUM_TASK);
  private final CountDownLatch adminReady = new CountDownLatch(1);


  @BeforeClass
  public void beforeClass() throws Exception {
    _participants = new MockParticipantManager[_numNodes];
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursively(namespace);
    }

    // Setup cluster and instances
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < _numNodes; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // start dummy participants
    for (int i = 0; i < _numNodes; i++) {
      final String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);

      // Set task callbacks
      Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
      taskFactoryReg.put("Backup", new DummyBackupTaskFactory(CLUSTER_NAME,
          Integer.parseInt(instanceName.split("_")[1])));

      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
    }

    // Start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Start an admin connection
    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _driver = new TaskDriver(_manager);

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
      for (TaskConfig taskConfig : _driver.getJobConfig(namespacedJobName).getTaskConfigMap()
          .values()) {
        Assert.assertEquals(taskConfig.getCommand(), "Backup");
        taskTargetParts.add(taskConfig.getTargetPartition());

        Assert.assertEquals(jobConfig.getJobCommandConfigMap().get("STORE_PATH_PREFIX"),
            _driver.getTaskUserContentMap(workflowName, jobName, "0").get("storePathPrefix"));

        Assert.assertEquals(jobConfig.getJobCommandConfigMap().get("BACKUP_LIMIT_MBS"),
            _driver.getTaskUserContentMap(workflowName, jobName, "0").get("backupLimitMbs"));

        Assert.assertEquals(jobConfig.getJobCommandConfigMap().get("RESOURCE_VERSION"),
            _driver.getTaskUserContentMap(workflowName, jobName, "0").get("resourceVersion"));
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

    public DummyBackupTaskFactory(String cluster, int adminPort) {
      super(cluster, adminPort);
    }

    @Override
    protected Task getTask(String cluster, String targetPartition, int backupLimitMbs,
                           String storePathPrefix, long resourceVersion, String job, int port) {
      return new DummyBackupTask(cluster, targetPartition, backupLimitMbs, storePathPrefix,
          resourceVersion, job, port);
    }
  }

  private class DummyBackupTask extends BackupTask {

    public DummyBackupTask(String taskCluster, String partitionName, int backupLimitMbs,
                           String storePathPrefix, long resourceVersion, String job,
                           int adminPort) {
      super(taskCluster, partitionName, backupLimitMbs, storePathPrefix, resourceVersion, job,
          adminPort);
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
        System.out.println(
            "Failed to read super class's private filed or fail to pur userStore" + e.getMessage());
      }

      return new TaskResult(TaskResult.Status.COMPLETED, "");
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
  }
}
