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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

@RunWith(PowerMockRunner.class)
public class TestDedupTaskFactory extends TaskTestBase {

  private static final String JOB_COMMAND = "DummyCommand";
  private static final int NUM_TASK = 2;
  private Map<String, String> _jobCommandMap;
  private long mockedLatestResourceVersion = 123456789L;

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

    _jobCommandMap = new HashMap<>();
  }

  @Test
  public void testDedupTaskFactoryCreateNewTaskWithSpecifiedResVersion() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    String resource_cluster = "test_cluster";
    String resource_segment = "test_segment";
    String resource_version = String.valueOf(System.currentTimeMillis());
    Set<String>
        target_partitions =
        new HashSet<>(Arrays.asList("test_segment_0", "test_segment_1"));

    // Create 1 jobs with 2 Dedup Task each
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int i = 0; i < NUM_TASK; i++) {
      Map<String, String> taskConfigMap = new HashMap<>();
      taskConfigMap.put("TASK_TARGET_PARTITION", resource_segment + "_" + String.valueOf(i));
      taskConfigs.add(new TaskConfig("Dedup", taskConfigMap));
    }

    _jobCommandMap.put("RESOURCE_CLUSTER", resource_cluster);
    _jobCommandMap.put("RESOURCE", resource_segment);
    _jobCommandMap.put("RESOURCE_VERSION", resource_version);
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
    // verify Task Command is passed in: Dedup
    for (TaskConfig taskConfig : _driver.getJobConfig(namespacedJobName).getTaskConfigMap()
        .values()) {
      Assert.assertEquals(taskConfig.getCommand(), "Dedup");
      Assert.assertTrue(target_partitions.contains(taskConfig.getTargetPartition()));
    }

    Set<String> jobVersions = new HashSet<>();

    // assert each task hold same resource version as job configured
    for (int i = 0; i < NUM_TASK; i++) {
      Map<String, String>
          taskContentMap =
          _driver.getTaskUserContentMap(workflowName, job, String.valueOf(i));
      Assert.assertEquals(taskContentMap.get("resourceCluster"), resource_cluster);
      Assert.assertEquals(taskContentMap.get("resourceName"), resource_segment);
      Assert.assertEquals(taskContentMap.get("resourceVersion"), resource_version);
      jobVersions.add(taskContentMap.get("jobVersion"));
    }
    Assert.assertEquals(jobVersions.size(), 1);

    _jobCommandMap.clear();
  }


  @Test
  public void testDedupTaskFactoryCreateNewTaskWithoutSpecifiedResVersion() throws Exception {
    String resource_cluster = "test_cluster";
    String resource_segment = "test_segment";

    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // Create 1 jobs with 2 Dedup Task each
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int i = 0; i < NUM_TASK; i++) {
      Map<String, String> taskConfigMap = new HashMap<>();
      taskConfigMap.put("TASK_TARGET_PARTITION", resource_segment + "_" + String.valueOf(i));
      taskConfigs.add(new TaskConfig("Dedup", taskConfigMap));
    }
    _jobCommandMap.put("RESOURCE_CLUSTER", resource_cluster);
    _jobCommandMap.put("RESOURCE", resource_segment);
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

    // assert task can dynamically get resource version job is not configed with resource version
    for (int i = 0; i < NUM_TASK; i++) {
      Map<String, String>
          taskContentMap =
          _driver.getTaskUserContentMap(workflowName, job, String.valueOf(i));
      Assert.assertEquals(taskContentMap.get("resourceCluster"), resource_cluster);
      Assert.assertEquals(taskContentMap.get("resourceName"), resource_segment);
      Assert.assertEquals(taskContentMap.get("resourceVersion"),
          String.valueOf(mockedLatestResourceVersion));
    }

    _jobCommandMap.clear();
  }

  //*****************************************************
  // dummy TaskFactory, Task to limit testing scope
  //
  // Varied behavior based on specific testing class:
  // - return same resource version
  // - populate Task level userStore to compared with Job Level userStore
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

      // store Dedup_limit_mbs, jobVersion into Task userStore to verify succesfully passed from
      // DedupTaskfactory
      try {
        // use Java reflection to access supper class's private members, only for testing
        String resCluster = readPrivateSuperClassStringField("resourceCluster");
        String resName = readPrivateSuperClassStringField("resourceName");
        long resVersion = readPrivateSuperClassLongField("resourceVersion");
        long jobVersion = readPrivateSuperClassLongField("jobVersion");
        putUserContent("resourceCluster", resCluster, Scope.TASK);
        putUserContent("resourceName", resName, Scope.TASK);
        putUserContent("resourceVersion", String.valueOf(resVersion), Scope.TASK);
        putUserContent("jobVersion", String.valueOf(jobVersion), Scope.TASK);
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
