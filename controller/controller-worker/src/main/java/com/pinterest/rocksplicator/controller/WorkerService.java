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
package com.pinterest.rocksplicator.controller;

import com.pinterest.rocksplicator.controller.mysql.MySQLClusterManager;
import com.pinterest.rocksplicator.controller.mysql.MySQLTaskQueue;
import com.pinterest.rocksplicator.controller.tasks.TaskFactory;
import com.pinterest.rocksplicator.controller.tasks.TaskModule;
import com.pinterest.rocksplicator.controller.util.AdminClientFactory;
import com.pinterest.rocksplicator.controller.util.EmailSender;
import com.pinterest.rocksplicator.controller.util.ShutdownHook;

import com.google.inject.Guice;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * The WorkerService contains:
 * (1) A task dispatcher to periodically pull data from MySQL task queue
 * (2) A pool of worker threads executing the tasks
 * To make sure:
 * (1) every worker thread can get task from dispatcher immediately (if there is idle worker thread)
 * (2) there will be no outstanding tasks pending for worker to pick up
 * It internally maintains a semaphore as the number of idle workers. Dispatcher will acquire()
 * the semaphore before getting any task, and will release() only if there is no outstanding
 * task from MySQL task queue. Workers will release() the semaphore whenever the task is executed
 * in afterExecute() hook.
 *
 * @author Shu Zhang (shu@pinterest.com)
 */
public class WorkerService {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerService.class);
  private static final ShutdownHook SHUTDOWN_HOOK = new ShutdownHook();

  public static void main(String[] args) {
    try {
      Runtime.getRuntime().addShutdownHook(new Thread(SHUTDOWN_HOOK));
      CuratorFramework zkClient = CuratorFrameworkFactory.newClient(
          WorkerConfig.getZKEndpoints(), new RetryOneTime(3000));
      zkClient.start();

      AdminClientFactory adminClientFactory = new AdminClientFactory(30);
      EmailSender emailSender = new EmailSender(WorkerConfig.getSenderEmailAddress(),
          WorkerConfig.getReceiverEmailAddress());
      ClusterManager clusterManager = new MySQLClusterManager(WorkerConfig.getJdbcUrl(),
          WorkerConfig.getMySqlUser(), WorkerConfig.getMySqlPassword());
      TaskFactory.setInjector(Guice.createInjector(
          new TaskModule(zkClient, adminClientFactory, emailSender, clusterManager)
      ));

      int workerPoolSize = WorkerConfig.getWorkerPoolSize();
      Semaphore idleWorkersSemaphore = new Semaphore(workerPoolSize);
      ThreadPoolExecutor threadPoolExecutor =
          new ThreadPoolExecutor(workerPoolSize, workerPoolSize, 0,
              TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
      TaskQueue taskQueue = new MySQLTaskQueue(WorkerConfig.getJdbcUrl(),
          WorkerConfig.getMySqlUser(), WorkerConfig.getMySqlPassword());
      WorkerPool workerPool = new WorkerPool(
          threadPoolExecutor, idleWorkersSemaphore, taskQueue, emailSender);
      TaskDispatcher dispatcher = new TaskDispatcher(
          WorkerConfig.getDispatcherPollIntervalSec(), idleWorkersSemaphore, workerPool, taskQueue);
      dispatcher.start();
      SHUTDOWN_HOOK.register(dispatcher::stop);
      SHUTDOWN_HOOK.register(adminClientFactory::shutdown);
      SHUTDOWN_HOOK.register(zkClient::close);
    } catch (Exception e) {
      LOG.error("Cannot start the worker service", e);
      System.exit(1);
    }
  }
}
