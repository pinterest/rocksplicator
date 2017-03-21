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
public final class TaskDispatcher {
  private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcher.class);
  private final long dispatcherPollIntervalSec;
  private boolean isRunning = false;
  private ScheduledExecutorService scheduler;
  private WorkerPool workerPool;
  private Semaphore idleWorkersSemaphore;
  private TaskQueue taskQueue;


  public TaskDispatcher(long dispatcherPollIntervalSec, Semaphore idleWorkersSemaphore,
                        WorkerPool workerPool, TaskQueue taskQueue) {
    this.dispatcherPollIntervalSec = dispatcherPollIntervalSec;
    this.idleWorkersSemaphore = idleWorkersSemaphore;
    this.workerPool = workerPool;
    this.taskQueue = taskQueue;
  }

  private void failTaskAndReleaseSemaphore(TaskInternal dequeuedTask,
                                           Semaphore idleWorkersSemaphore,
                                           String reason) {
    if (dequeuedTask != null && !taskQueue.failTask(dequeuedTask.id, reason)) {
      LOG.error("Cannot fail " + dequeuedTask.name + " to the queue");
    }
    idleWorkersSemaphore.release();
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
    scheduler = Executors.newSingleThreadScheduledExecutor();
    final Runnable dispatcher = new Runnable() {
      public void run() {
        try {
          while (true) {
            LOG.info("Pulling tasks from DB queue, available workers: "
                + idleWorkersSemaphore.availablePermits());
            idleWorkersSemaphore.acquire();
            TaskInternal dequeuedTask = taskQueue.dequeueTask(WorkerConfig.getHostName());
            if (dequeuedTask == null) {
              LOG.info("No outstanding pending tasks to be dequeued");
              failTaskAndReleaseSemaphore(dequeuedTask, idleWorkersSemaphore, null);
              break;
            } else {
              if (!workerPool.assignTask(dequeuedTask)) {
                failTaskAndReleaseSemaphore(dequeuedTask, idleWorkersSemaphore, "Failed to assign to worker");
                break;
              }
            }
          }
          scheduler.schedule(this, dispatcherPollIntervalSec, TimeUnit.SECONDS);
        } catch (Exception e) {
          LOG.error("Dispatcher is interrupted!", e);
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
