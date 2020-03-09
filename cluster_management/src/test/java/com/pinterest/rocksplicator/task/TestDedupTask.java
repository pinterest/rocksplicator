package com.pinterest.rocksplicator.task;

import com.pinterest.rocksplicator.TaskUtils;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

@RunWith(PowerMockRunner.class)
public class TestDedupTask extends TaskTestBase {

  private static final String JOB_COMMAND = "DummyCommand";
  private static final int NUM_TASK = 2;
  private Map<String, String> _jobCommandMap;
  private long mockedLatestResourceVersion = 123456789L;
  private long resourceVersionIncr = 1L;

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
  public void testDedupTasksInSameJobPossessUnifiedResourceVersion() throws Exception {

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

    adminReady.countDown();
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    String job = "JOB" + 0;
    String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, job);
    Map<String, String> jobUserStore = _driver.getJobUserContentMap(workflowName, job);

    Set<String> resVersionBeforeUnified = new HashSet<>();
    Set<String> jobVersionBeforeUnified = new HashSet<>();
    Set<String> resVersionAfterUnified = new HashSet<>();
    Set<String> jobVersionAfterUnified = new HashSet<>();
    for (int i = 0; i < NUM_TASK; i++) {
      Map<String, String>
          taskUserStore =
          _driver.getTaskUserContentMap(workflowName, job, String.valueOf(i));

      Assert.assertTrue(taskUserStore.containsKey("resourceVersion-beforeUnified"));
      resVersionBeforeUnified.add(taskUserStore.get("resourceVersion-beforeUnified"));
      Assert.assertTrue(taskUserStore.containsKey("jobVersion-beforeUnified"));
      jobVersionBeforeUnified.add(taskUserStore.get("jobVersion-beforeUnified"));

      Assert.assertTrue(taskUserStore.containsKey("resourceVersion-afterUnified"));
      resVersionAfterUnified.add(taskUserStore.get("resourceVersion-afterUnified"));
      Assert.assertTrue(taskUserStore.containsKey("jobVersion-afterUnified"));
      jobVersionAfterUnified.add(taskUserStore.get("jobVersion-afterUnified"));
    }

    // assert resource version diff before task execution
    Assert.assertTrue(jobUserStore.containsKey("resourceVersion"));
    Assert.assertEquals(resVersionBeforeUnified.size(), 2);
    Assert.assertEquals(resVersionAfterUnified.size(), 1);
    Assert.assertTrue(resVersionAfterUnified.contains(jobUserStore.get("resourceVersion")));

    // assert resource version unified after task execution
    Assert.assertTrue(jobUserStore.containsKey("jobVersion"));
    Assert.assertEquals(jobVersionBeforeUnified.size(), 1);
    Assert.assertEquals(jobVersionAfterUnified.size(), 1);
    Assert.assertTrue(jobVersionAfterUnified.contains(jobUserStore.get("jobVersion")));

    _jobCommandMap.clear();
  }

  //*****************************************************
  // dummy TaskFactory, Task to limit testing scope
  //
  // Varied behavior based on specific testing class:
  // - return varied resource version for diff Tasks from same Job, testing resource version
  // unifying during task execution
  // - populate Task level userStore
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
      return mockedLatestResourceVersion + resourceVersionIncr++;
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
      try {
        long resVersion = readPrivateSuperClassLongField("resourceVersion");
        long jobVersion = readPrivateSuperClassLongField("jobVersion");
        putUserContent("resourceVersion-beforeUnified", String.valueOf(resVersion), Scope.TASK);
        putUserContent("jobVersion-beforeUnified", String.valueOf(jobVersion), Scope.TASK);
      } catch (Exception e) {
        System.out.println(
            "Failed to read super class's private filed or fail to pur userStore" + e.getMessage());
      }

      super.run();

      // store resourceVersion, jobVersion, resourceCluster into Task userStore to compare with
      // Job level userStore populated by Dedup task execution
      try {
        // use Java reflection to access supper class's private members, only for testing
        long resVersion = readPrivateSuperClassLongField("resourceVersion");
        long jobVersion = readPrivateSuperClassLongField("jobVersion");
        String resCluster = readPrivateSuperClassStringField("resourceCluster");
        putUserContent("resourceVersion-afterUnified", String.valueOf(resVersion), Scope.TASK);
        putUserContent("jobVersion-afterUnified", String.valueOf(jobVersion), Scope.TASK);
        putUserContent("resourceCluster", resCluster, Scope.TASK);
      } catch (Exception e) {
        System.out.println(
            "Failed to read super class's private filed or fail to pur userStore" + e.getMessage());
      }

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
