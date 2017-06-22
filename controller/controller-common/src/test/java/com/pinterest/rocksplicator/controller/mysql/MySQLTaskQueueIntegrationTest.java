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

import com.pinterest.rocksplicator.controller.Task;
import com.pinterest.rocksplicator.controller.TaskBase;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.Set;

public class MySQLTaskQueueIntegrationTest {

  private static final String TEST_CLUSTER_NAME = "integ_test";
  private MySQLTaskQueue queue;

  @BeforeMethod
  protected void checkMySQLRunning() {
    try {
      this.queue = new MySQLTaskQueue("jdbc:mysql://localhost:3306/controller", "root", "");
    } catch (Exception e) {
      throw new SkipException("MySQL is not running correctly");
    }
    this.queue.createCluster(TEST_CLUSTER_NAME);
  }

  @AfterMethod
  protected void CleanUp() {
    this.queue.removeCluster(TEST_CLUSTER_NAME);
  }

  @Test
  public void testClusterTable() throws MySQLTaskQueue.MySQLTaskQueueException {
    Set<String> clusters = queue.getAllClusters();
    Assert.assertEquals(1, clusters.size());
    Assert.assertTrue(clusters.contains(TEST_CLUSTER_NAME));
    Assert.assertTrue(queue.lockCluster(TEST_CLUSTER_NAME));
    Assert.assertFalse(queue.lockCluster(TEST_CLUSTER_NAME));
    Assert.assertFalse(queue.removeCluster(TEST_CLUSTER_NAME));
    Assert.assertTrue(queue.unlockCluster(TEST_CLUSTER_NAME));
    Assert.assertTrue(queue.removeCluster(TEST_CLUSTER_NAME));
    Assert.assertFalse(queue.removeCluster(TEST_CLUSTER_NAME));
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
        queue.enqueueTask(createTaskBase("test-task", 1, "test-task-body"), TEST_CLUSTER_NAME, 10));
    Assert.assertFalse(
        queue.enqueueTask(createTaskBase("test-task", 1, "test-task-body"), "non-exist", 10));
  }

  @Test
  public void testDequeueAckTask() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p1", 1, "test-task-body-p1"),
        TEST_CLUSTER_NAME, 0));
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p0", 0, "test-task-body-p0"),
        TEST_CLUSTER_NAME, 0));
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p2", 2, "test-task-body-p2"),
        TEST_CLUSTER_NAME, 0));
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
            TEST_CLUSTER_NAME, 2));
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
        queue.enqueueTask(createTaskBase("test-task-zombie", 1, "test-task-body-p1"),
            TEST_CLUSTER_NAME, 0));
    Thread.sleep(2000);
    Task task = queue.dequeueTask("worker");
    Assert.assertEquals(task.name, "test-task-zombie");
    Assert.assertFalse(queue.lockCluster(TEST_CLUSTER_NAME));
    // make the task zombie
    Thread.sleep(2000);
    Assert.assertEquals(queue.resetZombieTasks(1), 1);
    Assert.assertTrue(queue.lockCluster(TEST_CLUSTER_NAME));
    Assert.assertTrue(queue.unlockCluster(TEST_CLUSTER_NAME));
    Task task2 = queue.dequeueTask("worker");
    Assert.assertEquals(task2.name, "test-task-zombie");
    Thread.sleep(1000);
    Assert.assertEquals(queue.resetZombieTasks(2), 0);
    queue.unlockCluster(TEST_CLUSTER_NAME);
  }

  @Test
  public void testKeepTasksAlive() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-alive", 1, "test-task-body-p1"),
            TEST_CLUSTER_NAME, 0));
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
            TEST_CLUSTER_NAME, 0));
    Thread.sleep(2000);
    Task task = queue.dequeueTask("worker");
    Assert.assertFalse(queue.lockCluster(TEST_CLUSTER_NAME));
    TaskBase newTask = createTaskBase("new-running-task", 0, "body");
    Assert.assertTrue(
        queue.finishTaskAndEnqueueRunningTask(task.id, "output", newTask, "worker") > 0);
    Assert.assertFalse(queue.lockCluster(TEST_CLUSTER_NAME));
    task = queue.peekTasks(TEST_CLUSTER_NAME, 2).get(0);
    Assert.assertEquals(task.name, "test-task-to-dequeue");
    task = queue.peekTasks(TEST_CLUSTER_NAME, 1).get(0);
    Assert.assertEquals(task.name, "new-running-task");
    Assert.assertTrue(queue.peekTasks(TEST_CLUSTER_NAME, 0).isEmpty());
    queue.finishTask(task.id, "done");
  }

  @Test
  public void testFinishTaskAndEnqueuePendingTask() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-to-dequeue", 1, "test-task-body-p1"),
            TEST_CLUSTER_NAME, 0));
    Thread.sleep(2000);
    Task task = queue.dequeueTask("worker");
    Assert.assertFalse(queue.lockCluster(TEST_CLUSTER_NAME));
    TaskBase newTask = createTaskBase("new-pending-task", 0, "body");
    Assert.assertTrue(queue.finishTaskAndEnqueuePendingTask(task.id, "output", newTask, 0));
    Assert.assertTrue(queue.lockCluster(TEST_CLUSTER_NAME));
    Assert.assertEquals(queue.dequeueTask("worker"), null);
    Assert.assertTrue(queue.unlockCluster(TEST_CLUSTER_NAME));
    task = queue.peekTasks(TEST_CLUSTER_NAME, 2).get(0);
    Assert.assertEquals(task.name, "test-task-to-dequeue");
    task = queue.peekTasks(TEST_CLUSTER_NAME, 0).get(0);
    Assert.assertEquals(task.name, "new-pending-task");
  }

  @Test
  public void testRemoveFinishedTasks() throws InterruptedException {
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p0", 0, "test-task-body-p0"),
            TEST_CLUSTER_NAME, 0));
    Thread.sleep(2000);
    Assert.assertTrue(
        queue.enqueueTask(createTaskBase("test-task-p2", 2, "test-task-body-p2"),
            TEST_CLUSTER_NAME, 0));
    Thread.sleep(2000);
    Task task1 = queue.dequeueTask("worker");
    Assert.assertTrue(queue.finishTask(task1.id, ""));
    Task task2 = queue.dequeueTask("worker");
    Assert.assertTrue(queue.finishTask(task2.id, ""));
    Assert.assertEquals(queue.removeFinishedTasks(3), 1);
  }

}
