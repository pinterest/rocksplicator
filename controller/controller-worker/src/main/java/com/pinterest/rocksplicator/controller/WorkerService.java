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
 * (1) every worker thread can get task from dispatcher immediately
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

  public static void main(String[] args) {
    try {
      WorkerConfig.initialize();
      int workerPoolSize = WorkerConfig.getWorkerPoolSize();
      Semaphore idleWorkersSemaphore = new Semaphore(workerPoolSize);
      ThreadPoolExecutor threadPoolExecutor =
          new ThreadPoolExecutor(workerPoolSize, workerPoolSize, 0,
              TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1));
      WorkerPool workerPool = new WorkerPool(threadPoolExecutor, idleWorkersSemaphore);
      TaskDispatcher dispatcher = new TaskDispatcher(
          WorkerConfig.getDispatcherPollInterval(), idleWorkersSemaphore, workerPool);
      dispatcher.start();
    } catch (Exception e) {
      LOG.error("Cannot start the worker service", e);
      System.exit(1);
    }
  }
}
