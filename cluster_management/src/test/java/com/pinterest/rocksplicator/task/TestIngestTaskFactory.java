package com.pinterest.rocksplicator.task;

import com.pinterest.rocksplicator.Utils;

import org.apache.helix.HelixAdmin;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class TestIngestTaskFactory extends TaskTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestIngestTaskFactory.class);

  // default test resource used by TaskSynchronizedTestBase
  private static final String TEST_RESOURCE = "TestDB";
  private static final String JOB_COMMAND = "DummyCommand";
  private static final int NUM_JOB = 1;
  private static final int NUM_TASK_PER_JOB = 1;
  private static final int NUM_TASK = NUM_JOB * NUM_TASK_PER_JOB;
  private Map<String, String> _jobCommandMap;

  private static final String fakeS3Bucket = "pinterest-fake-bucket";
  private static final String fakeRequestS3Bucket = "pinterest-fake-req-bucket";

  private final CountDownLatch allTasksReady = new CountDownLatch(NUM_TASK);
  private final CountDownLatch adminReady = new CountDownLatch(1);

  // local variables to store job/task execution context for verification use only
  private Set<String> instanceNames;
  private Set<String> ingestedPartitions;
  // how many times "executeIngest" called
  private AtomicInteger numExecuteIngest = new AtomicInteger(0);

  @Override
  protected void startParticipant(String zkAddr, int i) {
    final String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
    instanceNames.add(instanceName);
    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("Ingest",
        new DummyIngestTaskFactory(CLUSTER_NAME, instanceName.split("_")[0],
            Integer.parseInt(instanceName.split("_")[1]), fakeS3Bucket));
    this._participants[i] = new MockParticipantManager(zkAddr, this.CLUSTER_NAME, instanceName);
    StateMachineEngine stateMachine = this._participants[i].getStateMachineEngine();
    stateMachine.registerStateModelFactory(
        "Task", new TaskStateModelFactory(this._participants[i], taskFactoryReg));
    this._participants[i].syncStart();
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    // this will setup a cluster with 5 hosts; a resource "TestDB" with 20 partitions, 3 RF.
    _jobCommandMap = new HashMap<>();
    instanceNames = new HashSet<>();
    ingestedPartitions = new HashSet<>();
    numExecuteIngest.set(0);
    super.beforeClass();
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

      _jobCommandMap.put("S3_BUCKET", fakeRequestS3Bucket);
      _jobCommandMap.put("RESOURCE_STORE_PATH", "/test_cloud/");
      _jobCommandMap.put("INGEST_BEHIND", "true");
      _jobCommandMap.put("DOWNLOAD_LIMIT_MBS", "30");

      List<TaskConfig> taskConfigs = new ArrayList<>();
      for (int j = 0; j < NUM_TASK_PER_JOB; ++j) {
        Map<String, String> taskConfigMap = new HashMap<>();
        taskConfigMap.put("TASK_TARGET_PARTITION", String.format("%s_%d", TEST_RESOURCE, j));
        ingestedPartitions.add(String.format("%s_%d", TEST_RESOURCE, j));
        taskConfigs.add(new TaskConfig("Ingest", taskConfigMap));
      }

      JobConfig.Builder jobConfigBulider = new JobConfig.Builder()
          .setCommand(JOB_COMMAND)
          .addTaskConfigs(taskConfigs)
          .setJobCommandConfigMap(_jobCommandMap);
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
    instanceNames.clear();
    ingestedPartitions.clear();
    numExecuteIngest.set(0);
  }

  @Test
  public void testIngestTaskFactoryCreateNewTaskWithPassedConfigs() throws Exception {
    String workflowName = TestHelper.getTestMethodName();

    Assert.assertEquals(_driver.getWorkflowConfig(workflowName).getWorkflowId(), workflowName);

    for (int i = 0; i < NUM_JOB; i++) {
      String jobName = "JOB" + i;
      String namespacedJobName = TaskUtil.getNamespacedJobName(workflowName, jobName);
      JobConfig jobConfig = _driver.getJobConfig(namespacedJobName);

      // Verify Job confgis: when task cmd is provided; job_cmd remained as dummy, but, in reality,
      // task_cmd is executed
      Assert.assertEquals(jobConfig.getCommand(), JOB_COMMAND);
      Assert.assertEquals(jobConfig.getJobCommandConfigMap(), _jobCommandMap);

      // Verify task context
      // each task send out 3 rpcs to the 3 replica of the same partition
      Assert.assertEquals(numExecuteIngest.get(), NUM_TASK * 3);

      Set<String> taskTargetParts = new HashSet<>();
      for (Map.Entry<String, TaskConfig> entry : jobConfig.getTaskConfigMap().entrySet()) {
        String taskId = entry.getKey();
        TaskConfig taskConfig = entry.getValue();
        int taskPartitionId = getTaskPartitionIdFromTaskConfig(_driver, namespacedJobName, taskId);

        Assert.assertEquals(taskConfig.getCommand(), "Ingest");
        Assert.assertEquals(taskConfig.getId(), taskId);
        taskTargetParts.add(taskConfig.getTargetPartition());

        Assert.assertEquals(
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("taskCluster"),
            CLUSTER_NAME);
        Assert.assertEquals(
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("s3Bucket"),
            fakeRequestS3Bucket);
        Assert.assertTrue(instanceNames.contains(
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("instance")));
        Assert.assertEquals(
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("dbName"),
            Utils.getDbName(taskConfig.getTargetPartition()));
        Assert.assertEquals(
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("ingest_behind"),
            jobConfig.getJobCommandConfigMap().get("INGEST_BEHIND"));
        Assert.assertEquals(
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("downloadLimitMbs"),
            jobConfig.getJobCommandConfigMap().get("DOWNLOAD_LIMIT_MBS"));
        Assert.assertEquals(
            _driver.getTaskUserContentMap(workflowName, jobName, String.valueOf(taskPartitionId))
                .get("partitionStorePath"),
            jobConfig.getJobCommandConfigMap().get("RESOURCE_STORE_PATH")
                + Utils.getS3PartPrefix(taskConfig.getTargetPartition()));
      }

      Assert.assertEquals(taskTargetParts, ingestedPartitions);
    }
  }

  // helper funct to get the <taskParititionId>, e.g. 0, 1, ..., from taskId, e.g.
  // 428b8f5a-fd5a-4c44-ab7e-a353980941dd
  int getTaskPartitionIdFromTaskConfig(TaskDriver driver, String namespacedJobName, String taskId) {
    JobContext jobContext = _driver.getJobContext(namespacedJobName);
    Map<String, Integer> taskIdPartitionMap = jobContext.getTaskIdPartitionMap();
    return taskIdPartitionMap.get(taskId);
  }

  //*****************************************************
  // dummy TaskFactory, Task to limit testing scope
  //
  // Varied behavior based on specific testing class:
  // - populate Task level userStore
  //****************************************************/

  private class DummyIngestTaskFactory extends IngestTaskFactory {

    public DummyIngestTaskFactory(String cluster, String host, int adminPort, String s3Bucket) {
      super(cluster, host, adminPort, s3Bucket);
    }

    @Override
    protected Task getTask(HelixAdmin admin, String cluster, String targetPartition, String job,
                           boolean ingest_behind, String host, int port, int downloadLimitMbs,
                           String s3Bucket,
                           String partitionStorePath, TaskConfig taskConfig) {
      return new TestIngestTaskFactory.DummyIngestTask(admin, cluster, targetPartition, job,
          ingest_behind, host, port, downloadLimitMbs, s3Bucket, partitionStorePath, taskConfig);
    }
  }

  private class DummyIngestTask extends IngestTask {

    public DummyIngestTask(HelixAdmin admin, String taskCluster, String partitionName, String job,
                           boolean ingest_behind, String host, int adminPort, int downloadLimitMbs,
                           String s3Bucket,
                           String partitionStorePath, TaskConfig taskConfig) {
      super(admin, taskCluster, partitionName, job, ingest_behind, host, adminPort,
          downloadLimitMbs, s3Bucket, partitionStorePath, taskConfig);
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
        String taskCluster = readPrivateSuperClassStringField("taskCluster");
        String bucket = readPrivateSuperClassStringField("s3Bucket");
        putUserContent("taskCluster", taskCluster, Scope.TASK);
        putUserContent("s3Bucket", bucket, Scope.TASK);
      } catch (Exception e) {
        LOG.error(
            "Failed to read super class's private filed or fail to pur userStore" + e.getMessage());
      }

      try {
        super.run();
      } catch (Exception e) {
        LOG.error("Failed to execute IngestTask run" + e.getMessage());
      }

      return new TaskResult(TaskResult.Status.COMPLETED, "");
    }

    @Override
    protected void executeIngest(String host, int port, String dbName, boolean ingest_behind,
                                 int downloadLimitMbs, String s3Bucket, String partitionStorePath)
        throws RuntimeException {
      numExecuteIngest.incrementAndGet();
      try {
        putUserContent("instance", String.format("%s_%d", host, port), Scope.TASK);
        putUserContent("dbName", dbName, Scope.TASK);
        putUserContent("ingest_behind", String.valueOf(ingest_behind), Scope.TASK);
        putUserContent("downloadLimitMbs", String.valueOf(downloadLimitMbs), Scope.TASK);
        putUserContent("partitionStorePath", partitionStorePath, Scope.TASK);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void cancel() {}

    // helper function for testing
    // java refection: http://tutorials.jenkov.com/java-reflection/private-fields-and-methods.html

    private String readPrivateSuperClassStringField(String fieldName) throws Exception {
      Class<?> clazz = getClass().getSuperclass();
      Field field = clazz.getDeclaredField(fieldName);
      field.setAccessible(true);
      return (String) field.get(this);
    }
  }
}
