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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 *
 * Periodically querying MySQL for claiming tasks, checking abort signals, etc.
 *
 * @author Shu Zhang (shu@pinterest.com)
 *
 */
public class TaskDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class);
  private boolean isRunning = false;
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
  private final long dispatcherPollInterval;
  private WorkerPool workerPool;
  private volatile Semaphore idleWorkersSemaphore;


  public TaskDispatcher(long dispatcherPollInterval, Semaphore idleWorkersSemaphore,
                        WorkerPool workerPool) {
    this.dispatcherPollInterval = dispatcherPollInterval;
    this.idleWorkersSemaphore = idleWorkersSemaphore;
    this.workerPool = workerPool;
  }

  /**
   * Start to dispatch.
   * @return if start is succeeded.
   */
  public synchronized boolean start() {
    if (isRunning) {
      LOG.error("Dispatcher is already running, cannot start.");
      return false;
    }
    final Runnable dispatcher = new Runnable() {
      public void run() {
        try {
          idleWorkersSemaphore.acquire();
          // TODO: Add task dispatch logic to assign tasks to worker pool
          scheduler.schedule(this, dispatcherPollInterval, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          LOG.error("Dispatcher is interrupted!");
        }
      }
    };
    scheduler.submit(dispatcher);
    isRunning = true;
    return true;
  }

  /**
   * Stop to dispatch tasks.
   */
  public synchronized boolean stop() {
    if (!isRunning) {
      LOG.error("Dispatcher is not running, cannot stop.");
      return false;
    }
    scheduler.shutdown();
    isRunning = false;
    return true;
  }
}
