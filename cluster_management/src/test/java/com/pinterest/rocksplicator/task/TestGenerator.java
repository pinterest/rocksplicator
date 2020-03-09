package com.pinterest.rocksplicator.task;

import com.pinterest.rocksplicator.Generator;
import com.pinterest.rocksplicator.TaskUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.Task;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class TestGenerator extends TaskTestBase {
  private HelixAdmin admin;
  private static final String JOB_COMMAND = "DummyCommand";
  private static final int NUM_TASK = 2;
  private Map<String, String> _jobCommandMap;
  private long mockedLatestResourceVersion = 123456789L;

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
      taskFactoryReg.put("Dedup", new DummyDedupTaskFactory(instanceName.split("_")[0],
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
    admin = _manager.getClusterManagmentTool();

    _jobCommandMap = new HashMap<>();
  }

  @BeforeTest
  public void setup() throws Exception {

  }

  @Test
  public void TestGetDbResourcesByExcludingWfAndJobs() throws Exception {
    // add 1 res: TestDB with 20 partNum, and MasterSlave mode
    super.setupDBs();

    // add workflow
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // Create 1 jobs with 2 Dedup Task
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int i = 0; i < NUM_TASK; i++) {
      Map<String, String> taskConfigMap = new HashMap<>();
      taskConfigMap.put("TASK_TARGET_PARTITION", "test_segment_" + String.valueOf(i));
      taskConfigs.add(new TaskConfig("Dedup", taskConfigMap));
    }

    String resource_cluster = "test_cluster";
    String resource_segment = "test_segment";
    _jobCommandMap.put("RESOURCE_CLUSTER", resource_cluster);
    _jobCommandMap.put("RESOURCE", resource_segment);

    JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
        .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap);
    String jobName = "JOB" + 0;
    workflowBuilder.addJob(jobName, jobConfigBulider);

    // Start the workflow and wait for all tasks started
    _driver.start(workflowBuilder.build());
    allTasksReady.await();

    List<String> resources = admin.getResourcesInCluster(CLUSTER_NAME);
    // before job completd, resources contain: TestDB, [wf], [job]
    Assert.assertEquals(resources.size(), 3);

    adminReady.countDown();
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    DummyGenerator dummyGenerator = new DummyGenerator(CLUSTER_NAME, _manager);
    List<String> db_resources = dummyGenerator.filterOutWfAndJobs(resources);

    Assert.assertEquals(db_resources.size(), 1);
    Assert.assertTrue(db_resources.contains(WorkflowGenerator.DEFAULT_TGT_DB));

    // force remove the workflow and its jobs' related zk nodes. prevent affect other tests
    _driver.delete(workflowName, true);

    _jobCommandMap.clear();
  }

  @Test
  public void TestJobStateGeneration() throws Exception {
    // add workflow
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // Create 2 jobs with 1 Dedup Task
    String resource_cluster = "test_cluster";
    String resource_segment = "test_segment";
    for (int i = 0; i < NUM_TASK; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      Map<String, String> taskConfigMap = new HashMap<>();
      taskConfigMap.put("TASK_TARGET_PARTITION", resource_segment + i + "_" + String.valueOf(i));
      taskConfigs.add(new TaskConfig("Dedup", taskConfigMap));

      String jobName = "JOB" + i;
      Map<String, String> jobCmdMap = new HashMap<>();
      jobCmdMap.put("RESOURCE_CLUSTER", resource_cluster + i);
      jobCmdMap.put("RESOURCE", resource_segment + i);
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(jobCmdMap);
      workflowBuilder.addJob(jobName, jobConfigBulider);
    }

    // Start the workflow and wait for all tasks started
    _driver.start(workflowBuilder.build());
    allTasksReady.await();

    adminReady.countDown();
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    DummyGenerator dummyGenerator = new DummyGenerator(CLUSTER_NAME, _manager);
    String stateJson = dummyGenerator.getJobStateConfig();
    Map<String, Map<String, Map<String, String>>> stateMap = TaskUtils.jobStateJsonToMap(stateJson);

    // 2 segments are deduped
    Assert.assertEquals(stateMap.keySet(),
        new HashSet<String>(Arrays.asList("test_segment0", "test_segment1")));

    // the deduped resource version is the assigned version
    Assert.assertTrue(stateMap.get("test_segment0").keySet()
        .contains(String.valueOf(mockedLatestResourceVersion)));
    Assert.assertEquals(
        stateMap.get("test_segment0").get(String.valueOf(mockedLatestResourceVersion)).values(),
        new HashSet<String>(Arrays.asList("COMPLETED")));

    // force remove the workflow and its jobs' related zk nodes. prevent affect other tests
    _driver.delete(workflowName, true);
  }


  //*****************************************************
   // dummy TaskFactory, Task to limit testing scope
   //
   // Varied behavior based on specific testing class:
   // - return same resource version
   // - no populate Task level userStore
   //****************************************************/

  protected class DummyDedupTaskFactory extends DedupTaskFactory {

    public DummyDedupTaskFactory(String host, int adminPort, String zkConnectString,
                                 String cluster) {
      super(host, adminPort, zkConnectString, cluster);
    }

    @Override
    protected Task getTask(String cluster, String resourceCluster, String resourceName,
                           String targetPartition,
                           long resourceVersion, long jobVersion, String job,
                           String host, int port, CuratorFramework zkClient) {
      return new DummyDedupTask(cluster, resourceCluster, resourceName, targetPartition,
          resourceVersion,
          jobVersion, job, host, port, zkClient);
    }

    @Override
    protected long getLatestVersion(String cluster, String resource) {
      // return different resource version to tasks, test unifying version among tasks
      return mockedLatestResourceVersion;
    }
  }

  protected class DummyDedupTask extends DedupTask {

    public DummyDedupTask(String taskCluster, String resourceCluster, String resourceName,
                          String partitionName, long resourceVersion, long jobVersion,
                          String job, String host, int adminPort,
                          CuratorFramework zkClient) {
      super(taskCluster, resourceCluster, resourceName, partitionName, resourceVersion, jobVersion,
          job, host, adminPort, zkClient);
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

      return new TaskResult(TaskResult.Status.COMPLETED, "");
    }

    @Override
    protected TaskUtils.BackupStatus getBackupStatus(String cluster, String segment,
                                                     long resVersion) throws RuntimeException {

      return TaskUtils.BackupStatus.COMPLETED;
    }

    @Override
    protected TaskUtils.ExportStatus getExportStatus(String taskCluster, String segment,
                                                     long resourceVersion, long jobVersion)
        throws RuntimeException {
      return TaskUtils.ExportStatus.FAILED;
    }

    @Override
    protected void executeDedup(String dbName, int adminPort, String src_hdfsPath,
                                String dest_hdfsPath) throws RuntimeException {
      System.out.println("mock execute dedup");
    }

    @Override
    public void cancel() {
    }
  }

  // dummy Generator for testing public methods: @getJobStateConfig, @filterOutWfAndJobs
  private class DummyGenerator extends Generator {

    public DummyGenerator(String clusterName, HelixManager helixManager) {
      super(clusterName, helixManager);
      super.driver = _driver;
    }

    @Override
    public void onCallback(NotificationContext notificationContext) {

    }
  }

}
