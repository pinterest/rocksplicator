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

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.Set;

public class MySQLTaskQueueIntegrationTest {

  private static final String TEST_CLUSTER_NAME = "integ_test";
  private EntityManager entityManager;
  private MySQLTaskQueue queue;

  @BeforeMethod
  protected void checkMySQLRunning() {
    try {
      EntityManagerFactory entityManagerFactory =
          Persistence.createEntityManagerFactory("controller-test");
      this.entityManager = entityManagerFactory.createEntityManager();
      this.queue = new MySQLTaskQueue(entityManager);
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
    Assert.assertEquals("test-task-p0", task1.name);
    Assert.assertEquals(0, task1.priority);
    Assert.assertEquals(1, task1.state);
    Assert.assertTrue(queue.finishTask(task1.id, ""));
    Task task2 = queue.dequeueTask("worker");
    Assert.assertEquals("test-task-p1", task2.name);
    Assert.assertEquals(1, task2.priority);
    Assert.assertEquals(1, task2.state);
    Assert.assertTrue(queue.finishTask(task2.id, ""));
    Task task3 = queue.dequeueTask("worker");
    Assert.assertEquals("test-task-p2", task3.name);
    Assert.assertEquals(2, task3.priority);
    Assert.assertEquals(1, task3.state);
    Assert.assertTrue(queue.finishTask(task3.id, ""));
  }
}
