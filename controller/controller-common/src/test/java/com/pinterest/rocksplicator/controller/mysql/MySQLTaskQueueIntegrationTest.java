/*
 * Copyright 2017 Pinterest, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pinterest.rocksplicator.controller.mysql;

import com.pinterest.rocksplicator.controller.Cluster;
import com.pinterest.rocksplicator.controller.Task;
import com.pinterest.rocksplicator.controller.TaskBase;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.Set;

/**
 * To run the test, we need to create a DB called "controller_test" and run create_schemas.sh
 * against "controller_test":
 * sh tools/create_schemas.sh -d controller_test.
 */
public class MySQLTaskQueueIntegrationTest {

  private static final String TEST_CLUSTER_NAME = "integ_test";
  private static final String TEST_CLUSTER_NAMESPACE = "integ_test_rocksdb";
  private Cluster cluster = new Cluster(TEST_CLUSTER_NAMESPACE, TEST_CLUSTER_NAME);
  private MySQLTaskQueue queue;

  @BeforeMethod
  protected void checkMySQLRunning() {
    try {
      this.queue = new MySQLTaskQueue("jdbc:mysql://localhost:3306/controller_test", "root", "");
    } catch (Exception e) {
      throw new SkipException("MySQL is not running correctly");
    }
    this.queue.createCluster(cluster);
  }

  @AfterMethod
  protected void CleanUp() {
    this.queue.removeCluster(cluster);
  }

  @Test
  public void testClusterTable() throws MySQLTaskQueue.MySQLTaskQueueException {
    Set<Cluster> clusters = queue.getAllClusters();
    Assert.assertEquals(1, clusters.size());
    Assert.assertTrue(clusters.contains(cluster));
    Assert.assertTrue(queue.lockCluster(cluster));
    Assert.assertFalse(queue.lockCluster(cluster));
    Assert.assertFalse(queue.removeCluster(cluster));
    Assert.assertTrue(queue.unlockCluster(cluster));
    Assert.assertTrue(queue.removeCluster(cluster));
    Assert.assertFalse(queue.removeCluster(cluster));
  }

  TaskBase createTaskBase(String name, int priority, String body) {
    TaskBase taskBase = new TaskBase();
    taskBase.name = name;
    taskBase.priority = priority;
    taskBase.body = body;
    return taskBase;
  }

  @Test
  public void testEnqueueTask() {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task", 1, "test-task-body"), cluster, 10));
    Assert.assertFalse(
        queue.enqueueTask(createTaskBase("test-task", 1, "test-task-body"),
            new Cluster("nonexist", "nonexist"), 10));
  }

  @Test
  public void testDequeueAckTask() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p1", 1, "test-task-body-p1"),
        cluster, 0));
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p0", 0, "test-task-body-p0"),
        cluster, 0));
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p2", 2, "test-task-body-p2"),
        cluster, 0));
    Thread.sleep(2000);
    Task task1 = queue.dequeueTask("worker");
    Assert.assertEquals(task1.name, "test-task-p0");
    Assert.assertEquals(task1.priority, 0);
    Assert.assertEquals(task1.state, 1);
    Assert.assertTrue(queue.finishTask(task1.id, ""));
    Task task2 = queue.dequeueTask("worker");
    Assert.assertEquals(task2.name, "test-task-p1");
    Assert.assertEquals(task2.priority, 1);
    Assert.assertEquals(task2.state, 1);
    Assert.assertTrue(queue.finishTask(task2.id, ""));
    Task task3 = queue.dequeueTask("worker");
    Assert.assertEquals(task3.name, "test-task-p2");
    Assert.assertEquals(task3.priority, 2);
    Assert.assertEquals(task3.state, 1);
    Assert.assertTrue(queue.finishTask(task3.id, ""));
    // Test run delay seconds
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-delayed", 0, "test-task-body-delayed"),
            cluster, 2));
    Thread.sleep(1000);
    Assert.assertEquals(queue.dequeueTask("worker"), null);
    Thread.sleep(3000);
    Task task4 = queue.dequeueTask("worker");
    Assert.assertEquals(task4.name, "test-task-delayed");
    Assert.assertTrue(queue.finishTask(task4.id, ""));
  }

  @Test
  public void testResetZombieTasks() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-zombie", 1, "test-task-body-p1"), cluster, 0));
    Thread.sleep(2000);
    Task task = queue.dequeueTask("worker");
    Assert.assertEquals(task.name, "test-task-zombie");
    Assert.assertFalse(queue.lockCluster(cluster));
    // make the task zombie
    Thread.sleep(2000);
    Assert.assertEquals(queue.resetZombieTasks(1), 1);
    Assert.assertTrue(queue.lockCluster(cluster));
    Assert.assertTrue(queue.unlockCluster(cluster));
    Task task2 = queue.dequeueTask("worker");
    Assert.assertEquals(task2.name, "test-task-zombie");
    Thread.sleep(1000);
    Assert.assertEquals(queue.resetZombieTasks(2), 0);
    queue.unlockCluster(cluster);
  }

  @Test
  public void testKeepTasksAlive() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-alive", 1, "test-task-body-p1"),
            cluster, 0));
    Thread.sleep(2000);
    Task task = queue.dequeueTask("worker");
    Thread.sleep(2000);
    Date dateAlive = new Date();
    Assert.assertTrue(queue.keepTaskAlive(task.id));
    Task taskUpdated = queue.findTask(task.id);
    Assert.assertEquals(dateAlive.toString(), taskUpdated.lastAliveAt.toString());
    queue.finishTask(taskUpdated.id, "");
  }

  @Test
  public void testFinishTaskAndEnqueueRunningTask() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-to-dequeue", 1, "test-task-body-p1"),
            cluster, 0));
    Thread.sleep(2000);
    Task task = queue.dequeueTask("worker");
    Assert.assertFalse(queue.lockCluster(cluster));
    TaskBase newTask = createTaskBase("new-running-task", 0, "body");
    Assert.assertTrue(
        queue.finishTaskAndEnqueueRunningTask(task.id, "output", newTask, "worker") > 0);
    Assert.assertFalse(queue.lockCluster(cluster));
    task = queue.peekTasks(cluster, 2).get(0);
    Assert.assertEquals(task.name, "test-task-to-dequeue");
    task = queue.peekTasks(cluster, 1).get(0);
    Assert.assertEquals(task.name, "new-running-task");
    Assert.assertTrue(queue.peekTasks(cluster, 0).isEmpty());
    queue.finishTask(task.id, "done");
  }

  @Test
  public void testFinishTaskAndEnqueuePendingTask() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-to-dequeue", 1, "test-task-body-p1"),
            cluster, 0));
    Thread.sleep(2000);
    Task task = queue.dequeueTask("worker");
    Assert.assertFalse(queue.lockCluster(cluster));
    TaskBase newTask = createTaskBase("new-pending-task", 0, "body");
    Assert.assertTrue(queue.finishTaskAndEnqueuePendingTask(task.id, "output", newTask, 0));
    Assert.assertTrue(queue.lockCluster(cluster));
    Assert.assertEquals(queue.dequeueTask("worker"), null);
    Assert.assertTrue(queue.unlockCluster(cluster));
    task = queue.peekTasks(cluster, 2).get(0);
    Assert.assertEquals(task.name, "test-task-to-dequeue");
    task = queue.peekTasks(cluster, 0).get(0);
    Assert.assertEquals(task.name, "new-pending-task");
  }

  @Test
  public void testRemoveFinishedTasksBasic() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p0", 0, "test-task-body-p0"),
            cluster, 0));
    Thread.sleep(2000);
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p2", 2, "test-task-body-p2"),
            cluster, 0));
    Thread.sleep(2000);
    Task task1 = queue.dequeueTask("worker");
    Assert.assertTrue(queue.finishTask(task1.id, ""));
    Task task2 = queue.dequeueTask("worker");
    Assert.assertTrue(queue.finishTask(task2.id, ""));
    Assert.assertEquals(queue.removeFinishedTasks(3, null, null, null), 1);
  }

  @Test
  public void testRemoveFinishedTasksWithTaskName() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p0", 0, "test-task-body-p0"),
            cluster, 0));
    Thread.sleep(2000);
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p2", 2, "test-task-body-p2"),
            cluster, 0));
    Thread.sleep(2000);
    Task task1 = queue.dequeueTask("worker");
    Assert.assertTrue(queue.finishTask(task1.id, ""));
    Task task2 = queue.dequeueTask("worker");
    Assert.assertTrue(queue.finishTask(task2.id, ""));
    Assert.assertEquals(queue.removeFinishedTasks(1, null, null, "test-task-p0"), 1);
  }

  @Test
  public void testRemoveFinishedTasksWithNameFromCluster() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p0", 0, "test-task-body-p0"),
            cluster, 0));
    Thread.sleep(2000);
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p2", 2, "test-task-body-p2"),
            cluster, 0));
    Thread.sleep(2000);
    Task task1 = queue.dequeueTask("worker");
    Assert.assertTrue(queue.finishTask(task1.id, ""));
    Task task2 = queue.dequeueTask("worker");
    Assert.assertTrue(queue.finishTask(task2.id, ""));
    Assert.assertEquals(queue.removeFinishedTasks(
        1, TEST_CLUSTER_NAMESPACE, TEST_CLUSTER_NAME, "test-task-p0"), 1);
  }

  @Test
  public void testPeekTasks() throws InterruptedException {
    Cluster realpin_p2p = new Cluster("realpin", "p2p");
    Cluster realpin_pinnability = new Cluster("realpin", "pinnability");
    Cluster aperture = new Cluster("rocksdb", "aperture");
    this.queue.createCluster(realpin_p2p);
    this.queue.createCluster(realpin_pinnability);
    this.queue.createCluster(aperture);
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("realpin-p2p", 1, "realpin-p2p-body"), realpin_p2p, 0));
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase(
            "realpin-pinnability", 1, "realpin-pinnability-body"), realpin_pinnability, 0));
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("apreture", 1, "aperture-body"), aperture, 0));
    Assert.assertEquals(this.queue.peekTasks(realpin_p2p, 0).size(), 1);
    Assert.assertEquals(this.queue.peekTasks(realpin_pinnability, 0).size(), 1);
    Assert.assertEquals(this.queue.peekTasks(aperture, 0).size(), 1);
    Assert.assertEquals(this.queue.peekTasks(realpin_p2p, null).size(), 1);
    Assert.assertEquals(this.queue.peekTasks(realpin_pinnability, null).size(), 1);
    Assert.assertEquals(this.queue.peekTasks(aperture, null).size(), 1);
    Assert.assertEquals(this.queue.peekTasks(new Cluster("realpin", ""), 0).size(), 2);
    Assert.assertEquals(this.queue.peekTasks(new Cluster("rocksdb", ""), 0).size(), 1);
    Assert.assertEquals(this.queue.peekTasks(new Cluster("realpin", ""), null).size(), 2);
    Assert.assertEquals(this.queue.peekTasks(new Cluster("rocksdb", ""), null).size(), 1);

    this.queue.removeCluster(realpin_p2p);
    this.queue.removeCluster(realpin_pinnability);
    this.queue.removeCluster(aperture);
  }

}
