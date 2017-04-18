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

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.Set;

public class MySQLTaskQueueIntegrationTest {

  private EntityManager entityManager;

  @BeforeTest
  protected void checkMySQLRunning() {
    try {
      EntityManagerFactory entityManagerFactory =
          Persistence.createEntityManagerFactory("controller-test");
      this.entityManager = entityManagerFactory.createEntityManager();
    } catch (Exception e) {
      throw new SkipException("MySQL is not running correctly");
    }
  }

  @Test
  public void testClusterTable() throws MySQLTaskQueue.MySQLTaskQueueException {
    MySQLTaskQueue queue = new MySQLTaskQueue(entityManager);
    String testCluster = "integ_test";
    Assert.assertFalse(queue.lockCluster(testCluster));
    Assert.assertTrue(queue.createCluster(testCluster));
    Set<String> clusters = queue.getAllClusters();
    Assert.assertEquals(1, clusters.size());
    Assert.assertTrue(clusters.contains(testCluster));
    Assert.assertTrue(queue.lockCluster(testCluster));
    Assert.assertFalse(queue.lockCluster(testCluster));
    Assert.assertFalse(queue.removeCluster(testCluster));
    Assert.assertTrue(queue.unlockCluster(testCluster));
    Assert.assertTrue(queue.removeCluster(testCluster));
    Assert.assertFalse(queue.removeCluster(testCluster));
  }
}
