/*
 *  Copyright 2017 Pinterest, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.pinterest.rocksplicator.controller.tasks;

import com.pinterest.rocksdb_admin.thrift.Admin;
import com.pinterest.rocksplicator.controller.FIFOTaskQueue;
import com.pinterest.rocksplicator.controller.util.AdminClientFactory;
import com.pinterest.rocksplicator.controller.util.ZKUtil;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.thrift.TException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class HealthCheckTaskTest {

  private static final String CLUSTER = "devtest";
  private static final String CONFIG =
      "{" +
      "  \"user_pins\": {" +
      "  \"num_shards\": 3," +
      "  \"127.0.0.1:8090:us-east-1a\": [\"00000:M\", \"00001:S\", \"00002:S\"]," +
      "  \"127.0.0.1:8091:us-east-1c\": [\"00000:S\", \"00001:M\", \"00002:S\"]," +
      "  \"127.0.0.1:8092:us-east-1e\": [\"00000:S\", \"00001:S\", \"00002:M\"]" +
      "   }" +
      "}";

  private TestingServer zkServer;
  private CuratorFramework zkClient;
  private Injector injector;

  @Mock private Admin.Client client;
  @Mock private AdminClientFactory clientFactory;


  @BeforeMethod
  public void setup() throws Exception {

    MockitoAnnotations.initMocks(this);

    zkServer = new TestingServer();
    zkClient = CuratorFrameworkFactory.newClient(
        zkServer.getConnectString(), new RetryOneTime(3000));
    zkClient.start();

    zkClient.createContainers(ZKUtil.getClusterConfigZKPath(CLUSTER));
    zkClient.setData().forPath(ZKUtil.getClusterConfigZKPath(CLUSTER), CONFIG.getBytes());

    TaskModule module = new TaskModule(zkClient, clientFactory);
    injector = Guice.createInjector(module);
  }

  @AfterMethod
  public void tearDown() throws IOException {
    zkClient.close();
    zkServer.close();
  }

  @Test
  public void testSuccessful() throws Exception {
    when(clientFactory.getClient(any())).thenReturn(client);

    HealthCheckTask t = new HealthCheckTask(
        new HealthCheckTask.Param().setNumReplicas(3)
    );
    injector.injectMembers(t);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    t.process(ctx);

    Assert.assertEquals(taskQueue.getResult(123), "Cluster devtest is healthy");
  }

  @Test
  public void testFail() throws Exception {
    doThrow(new TException()).when(client).ping();
    when(clientFactory.getClient(any())).thenReturn(client);

    HealthCheckTask t = new HealthCheckTask(
        new HealthCheckTask.Param().setNumReplicas(3)
    );
    injector.injectMembers(t);

    FIFOTaskQueue taskQueue = new FIFOTaskQueue(10);
    Context ctx = new Context(123, CLUSTER, taskQueue, null);
    t.process(ctx);

    Assert.assertTrue(taskQueue.getResult(123).startsWith("Unable to ping hosts"));
    Assert.assertTrue(taskQueue.getResult(123).contains("127.0.0.1:8090"));
    Assert.assertTrue(taskQueue.getResult(123).contains("127.0.0.1:8091"));
    Assert.assertTrue(taskQueue.getResult(123).contains("127.0.0.1:8092"));
  }

}
