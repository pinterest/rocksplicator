package com.pinterest.rocksplicator.task;

import org.apache.curator.framework.CuratorFramework;
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
import org.junit.Before;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.testng.Assert;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

@RunWith(PowerMockRunner.class)
public class TestBackupTaskFactory extends TaskTestBase {

  private static final String JOB_COMMAND = "DummyCommand";
  private static final int NUM_TASK = 2;
  private Map<String, String> _jobCommandMap;

  private final CountDownLatch allTasksReady = new CountDownLatch(NUM_TASK);
  private final CountDownLatch adminReady = new CountDownLatch(1);


  @Before
  public void setUp() throws Exception {

  }

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
      taskFactoryReg.put("Backup", new DummyBackupTaskFactory(instanceName.split("_")[0],
          Integer.parseInt(instanceName.split("_")[1]),
          ZK_ADDR, CLUSTER_NAME));

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

  @Test
  public void testBackupTaskFactoryCreateNewTask() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // Create 2 jobs with 1 Backup Task each
    for (int i = 0; i < NUM_TASK; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      Map<String, String> taskConfigMap = new HashMap<>();
      taskConfigMap.put("TASK_TARGET_PARTITION", "test_segment_" + String.valueOf(i));
      taskConfigs.add(new TaskConfig("Backup", taskConfigMap));
      _jobCommandMap.put("BACKUP_LIMIT_MBS", String.valueOf(10));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap);
      String jobName = "JOB" + i;
      workflowBuilder.addJob(jobName, jobConfigBulider);
    }

    // Start the workflow and wait for all tasks started
    _driver.start(workflowBuilder.build());
    allTasksReady.await();

    adminReady.countDown();
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    Assert.assertEquals(_driver.getWorkflowConfig(workflowName).getWorkflowId(), workflowName);
    for (int i = 0; i < NUM_TASK; i++) {
      String job = "JOB" + i;
      String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, job);
      // verify Job Command remained as dummy
      Assert.assertEquals(_driver.getJobConfig(namespacedJobName).getCommand(), JOB_COMMAND);
      // verify Task Command is passed in: Backup
      for (TaskConfig taskConfig : _driver.getJobConfig(namespacedJobName).getTaskConfigMap()
          .values()) {
        Assert.assertEquals(taskConfig.getCommand(), "Backup");
        Assert.assertEquals(taskConfig.getTargetPartition(), "test_segment_" + i);
      }
    }
  }

  @Test
  public void testJobVersionDifferAmongTasksFromDiffJobs() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // Create 2 jobs with 1 Backup Task each
    for (int i = 0; i < NUM_TASK; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      Map<String, String> taskConfigMap = new HashMap<>();
      taskConfigMap.put("TASK_TARGET_PARTITION", "test_segment_" + String.valueOf(i));
      taskConfigs.add(new TaskConfig("Backup", taskConfigMap));
      _jobCommandMap.put("BACKUP_LIMIT_MBS", String.valueOf(10));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap);
      String jobName = "JOB" + i;
      workflowBuilder.addJob(jobName, jobConfigBulider);
    }

    // Start the workflow and wait for all tasks started
    _driver.start(workflowBuilder.build());
    allTasksReady.await();

    adminReady.countDown();
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    Set<String> jobVersions = new HashSet<>();
    for (int i = 0; i < NUM_TASK; i++) {
      String job = "JOB" + i;
      // note: for backup, resVersion == jobVersion
      String resVersion = _driver.getTaskUserContentMap(workflowName, job, "0")
          .get("resourceVersion");
      // assert diff resVersions
      Assert.assertTrue(jobVersions.add(resVersion));
    }
    Assert.assertEquals(jobVersions.size(), 2);
  }

  @Test
  public void testJobVersionSameAmongTasksFromSameJob() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // Create 1 jobs with 2 Backup Task
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int i = 0; i < NUM_TASK; i++) {
      Map<String, String> taskConfigMap = new HashMap<>();
      taskConfigMap.put("TASK_TARGET_PARTITION", "test_segment_" + String.valueOf(i));
      taskConfigs.add(new TaskConfig("Backup", taskConfigMap));
    }
    _jobCommandMap.put("BACKUP_LIMIT_MBS", String.valueOf(10));
    JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
        .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap);
    String jobName = "JOB" + 0;
    workflowBuilder.addJob(jobName, jobConfigBulider);

    // Start the workflow and wait for all tasks started
    _driver.start(workflowBuilder.build());
    allTasksReady.await();

    adminReady.countDown();
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    Set<String> jobVersions = new HashSet<>();
    String job = "JOB" + 0;
    for (int i = 0; i < NUM_TASK; i++) {
      jobVersions.add(_driver.getTaskUserContentMap(workflowName, job, "0")
          .get("resourceVersion"));
    }
    Assert.assertEquals(jobVersions.size(), 1);
  }


  //*****************************************************
  // dummy TaskFactory, Task to limit testing scope
  //
  // Varied behavior based on specific testing class:
  // - populate Task level userStore
  //****************************************************/

  private class DummyBackupTaskFactory extends BackupTaskFactory {

    public DummyBackupTaskFactory(String host, int adminPort, String zkConnectString,
                                  String cluster) {
      super(host, adminPort, zkConnectString, cluster);
    }

    @Override
    protected Task getTask(String cluster, String resourceName, String targetPartition,
                           int backupLimitMbs,
                           long jobVersion, String job,
                           String host, int port, CuratorFramework zkClient) {
      return new DummyBackupTask(cluster, resourceName, targetPartition, backupLimitMbs, jobVersion,
          job, host,
          port,
          zkClient);
    }
  }

  private class DummyBackupTask extends BackupTask {

    public DummyBackupTask(String cluster, String resourceName, String partitionName,
                           int backupLimitMbs,
                           long resourceVersion,
                           String job, String host, int adminPort,
                           CuratorFramework zkClient) {
      super(cluster, resourceName, partitionName, backupLimitMbs, resourceVersion, job, host,
          adminPort, zkClient);
    }

    @Override
    public TaskResult run() {
      allTasksReady.countDown();
      try {
        adminReady.await();
      } catch (Exception e) {
        return new TaskResult(TaskResult.Status.FATAL_FAILED, e.getMessage());
      }

      // store backup_limit_mbs, jobVersion into Task userStore to verify succesfully passed from
      // BackupTaskfactory
      try {
        // use Java reflection to access supper class's private members, only for testing
        long resVersion = readPrivateSuperClassLongField("resourceVersion");
        int backupLimitMbs = readPrivateSuperClassIntField("backupLimitMbs");
        putUserContent("resourceVersion", String.valueOf(resVersion), Scope.TASK);
        putUserContent("backupLimitMbs", String.valueOf(backupLimitMbs), Scope.TASK);
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
  }
}
