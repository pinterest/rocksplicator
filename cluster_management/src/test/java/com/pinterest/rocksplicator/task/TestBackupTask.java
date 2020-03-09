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
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@RunWith(PowerMockRunner.class)
public class TestBackupTask extends TaskTestBase {

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
  public void testBackupTasksInSameJobPossessUnifiedResourceVersion() throws Exception {
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

    String job = "JOB" + 0;
    String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, job);
    Map<String, String> jobUserStore = _driver.getJobUserContentMap(workflowName, job);
    for (int i = 0; i < NUM_TASK; i++) {
      // verify Job Command remained as dummy
      Assert.assertEquals(_driver.getJobConfig(namespacedJobName).getCommand(), JOB_COMMAND);
      // verify Task Command is passed in: Backup
      Map<String, String>
          taskUserStore =
          _driver.getTaskUserContentMap(workflowName, job, String.valueOf(i));
      for (String store : taskUserStore.keySet()) {
        Assert.assertTrue(jobUserStore.containsKey(store));
        Assert.assertEquals(jobUserStore.get(store), taskUserStore.get(store));
      }
    }
    _jobCommandMap.clear();
  }


  //*****************************************************
  // dummy TaskFactory, Task to limit testing scope
  //
  // Varied behavior based on specific testing class:
  // - populate Task level userStore
  //****************************************************/

  protected class DummyBackupTaskFactory extends BackupTaskFactory {

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

  protected class DummyBackupTask extends BackupTask {

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
      super.run();

      // store resourceVersion, jobVersion, resourceCluster into Task userStore to compare with
      // Job level userStore populated by backup task execution
      try {
        // use Java reflection to access supper class's private members, only for testing
        long resVersion = readPrivateSuperClassLongField("resourceVersion");
        // resVersion == jobVersion
        long jobVersion = resVersion;
        // taskCluster == resourceCluster
        String resCluster = readPrivateSuperClassStringField("taskCluster");
        putUserContent("resourceVersion", String.valueOf(resVersion), Scope.TASK);
        putUserContent("jobVersion", String.valueOf(jobVersion), Scope.TASK);
        putUserContent("resourceCluster", resCluster, Scope.TASK);
      } catch (Exception e) {
        System.out.println(
            "Failed to read super class's private filed or fail to pur userStore" + e.getMessage());
      }

      return new TaskResult(TaskResult.Status.COMPLETED, "");
    }

    @Override
    protected void executeBackup(String host, int port, String dbName, String hdfsPath,
                                 int backupLimitMbs) throws RuntimeException {
      System.out.println("mock execute backup");
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
