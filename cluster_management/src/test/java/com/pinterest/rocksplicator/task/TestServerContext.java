package com.pinterest.rocksplicator.task;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.doReturn;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.tools.ClusterSetup;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


@RunWith(PowerMockRunner.class)
@PrepareForTest({CuratorFrameworkFactory.class})
public class TestServerContext extends TaskTestBase {

  private static final String JOB_COMMAND = "DummyCommand";
  private static final int NUM_JOB = 5;
  private Map<String, String> _jobCommandMap;

  private final CountDownLatch allTasksReady = new CountDownLatch(NUM_JOB);
  private final CountDownLatch adminReady = new CountDownLatch(1);

  private ExponentialBackoffRetry mockRetry;
  private CuratorFramework mockZkClient;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    mockRetry = mock(ExponentialBackoffRetry.class);
    PowerMockito.whenNew(ExponentialBackoffRetry.class).withAnyArguments().thenReturn(mockRetry);

    mockZkClient = mock(CuratorFramework.class);
    PowerMockito.spy(CuratorFrameworkFactory.class);
    doReturn(mockZkClient).when(
        CuratorFrameworkFactory.class, "newClient", anyString(), mockRetry);
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

      Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
      TaskFactory shortTaskFactory = new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new WriteTask(context);
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

    _jobCommandMap = new HashMap<>();
  }

  @Test
  public void testServerContextAccessZk() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // Create 5 jobs with 1 write Task
    for (int i = 0; i < NUM_JOB; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      taskConfigs.add(new TaskConfig("WriteTask", new HashMap<String, String>()));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap);
      String jobName = "JOB" + i;
      String taskPartitionId = "0";
      workflowBuilder.addJob(jobName, jobConfigBulider);
    }

    // Start the workflow and wait for all tasks started
    _driver.start(workflowBuilder.build());
    allTasksReady.await();

    adminReady.countDown();
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    ServerContext serverContext = new ServerContext(ZK_ADDR);
    TaskDriver taskDriver = serverContext.getTaskDriver(CLUSTER_NAME);
    Map<String, WorkflowConfig> workflowConfigMap = taskDriver.getWorkflows();
    Map<String, List<String>> dataMap = new HashMap<>();
    dataMap.put("workflows", new ArrayList<>(workflowConfigMap.keySet()));
    for (String wf : dataMap.get("workflows")) {
      System.out.println("Inside spectator, task driver dump workflowContext: " +
          taskDriver.getWorkflowContext(wf));
    }

    Assert.assertEquals(_driver.getJobConfig(workflowName + "_JOB0"), taskDriver.getJobConfig(workflowName + "_JOB0"));
  }

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
}

