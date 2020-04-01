package com.pinterest.rocksplicator;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.IdealState;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.Workflow;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowConfig;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class TestConfigGenerator extends TaskTestBase {

  private HelixAdmin admin;
  private static final String JOB_COMMAND = "DummyCommand";
  private static final int NUM_TASK = 2;
  private Map<String, String> _jobCommandMap;

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
      TaskFactory shortTaskFactory = new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new TestConfigGenerator.WriteTask(context);
        }
      };
      taskFactoryReg.put("WriteTask", shortTaskFactory);

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
    admin = _manager.getClusterManagmentTool();

    _jobCommandMap = new HashMap<>();
  }

  @BeforeMethod
  public void setup(Method m) throws Exception {
    String workflowName = m.getName();
    // add 1 res: TestDB with 20 partNum, and MasterSlave mode
    super.setupDBs();

    // add workflow
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // Create 1 jobs with 2 Task
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int i = 0; i < NUM_TASK; i++) {
      Map<String, String> taskConfigMap = new HashMap<>();
      taskConfigMap.put("TASK_TARGET_PARTITION", "test_segment_" + String.valueOf(i));
      taskConfigs.add(new TaskConfig("WriteTask", taskConfigMap));
    }

    JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
        .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap);
    String jobName = "JOB" + 0;
    workflowBuilder.addJob(jobName, jobConfigBulider);

    // Start the workflow and wait for all tasks started
    _driver.start(workflowBuilder.build());
    allTasksReady.await();
  }

  @AfterMethod
  public void cleanUp(Method m) throws Exception {
    _jobCommandMap.clear();
  }


  @Test
  public void TestGetDbResourcesByExcludingWfAndJobs() throws Exception {
    String workflowName = TestHelper.getTestMethodName();

    DummyGenerator dummyGenerator = new DummyGenerator(CLUSTER_NAME, _manager);

    List<String> resourcesBeforeJobDone = admin.getResourcesInCluster(CLUSTER_NAME);
    // before job completd, resources contain: TestDB, [wf], ongoing [job]
    Assert.assertEquals(resourcesBeforeJobDone.size(), 3);
    dummyGenerator.filterOutTaskResources(resourcesBeforeJobDone);
    Assert.assertEquals(resourcesBeforeJobDone.size(), 1);

    adminReady.countDown();
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    List<String> resourcesAfterJobDone = admin.getResourcesInCluster(CLUSTER_NAME);
    dummyGenerator.filterOutTaskResources(resourcesAfterJobDone);
    Assert.assertEquals(resourcesAfterJobDone.size(), 1);
    Assert.assertTrue(resourcesAfterJobDone.contains(WorkflowGenerator.DEFAULT_TGT_DB));
  }

  //*****************************************************
  // dummy TaskFactory, Task to limit testing scope
  //****************************************************/


  /**
   * A mock task that writes to UserContentStore. MockTask extends UserContentStore.
   */
  private class WriteTask extends MockTask {

    public WriteTask(TaskCallbackContext context) {
      super(context);
    }

    @Override
    public TaskResult run() {
      allTasksReady.countDown();
      try {
        adminReady.await();
      } catch (Exception e) {
        return new TaskResult(TaskResult.Status.FATAL_FAILED, e.getMessage());
      }
      return new TaskResult(TaskResult.Status.COMPLETED, "");
    }
  }


  // dummy Generator for testing public methods: @getJobStateConfig, @filterOutWfAndJobs
  private class DummyGenerator extends ConfigGenerator {

    public DummyGenerator(String clusterName, HelixManager helixManager) {
      super(clusterName, helixManager, "");
    }

    @Override
    public void onCallback(NotificationContext notificationContext) {

    }
  }

}