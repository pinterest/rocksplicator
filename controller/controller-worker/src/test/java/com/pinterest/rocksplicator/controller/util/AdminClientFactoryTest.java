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

package com.pinterest.rocksplicator.controller.util;

import com.pinterest.rocksdb_admin.thrift.Admin;

import junit.framework.Assert;
import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;


/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class AdminClientFactoryTest {

  private ServerSocket serverSocket;
  private TServer server;
  private Thread serverThread;

  private AdminClientFactory clientFactory;

  @Mock
  private Admin.Iface handler;

  @BeforeMethod
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);

    clientFactory = new AdminClientFactory(10);
    serverSocket = new ServerSocket(0);
    TServerTransport serverTransport = new TServerSocket(serverSocket);
    Admin.Processor processor = new Admin.Processor<>(handler);
    server = new TSimpleServer(new TServer.Args(serverTransport).processor(processor));

    serverThread  = new Thread(() -> server.serve());
    serverThread.start();

    // wait until server started
    while (!server.isServing()) {
      Thread.sleep(10);
    }
  }

  @AfterMethod
  public void tearDown() throws InterruptedException {
    clientFactory.shutdown();
    server.stop();
    serverThread.join();
  }

  @Test
  public void testCreateClient() throws Exception {
    for (int i = 0; i < 100; i++) {
      Admin.Client client = clientFactory.getClient(
          new InetSocketAddress("localhost", serverSocket.getLocalPort()));
      client.ping();
    }
    Assert.assertEquals(clientFactory.getClientPool().size(), 1);
  }

  @Test
  public void testInvalidateClosedSocket() throws Exception {
    Admin.Client client = clientFactory.getClient(
        new InetSocketAddress("localhost", serverSocket.getLocalPort()));
    client.ping();

    // close the underlying socket
    clientFactory.getClientPool()
        .get(new InetSocketAddress("localhost", serverSocket.getLocalPort())).close();

    try {
      client.ping();
      Assert.fail("Should have thrown exception when using bad client.");
    } catch (TException ex) {
      // continue
    }

    // renew client and ping again
    client = clientFactory.getClient(
        new InetSocketAddress("localhost", serverSocket.getLocalPort()));
    client.ping();
  }
}
