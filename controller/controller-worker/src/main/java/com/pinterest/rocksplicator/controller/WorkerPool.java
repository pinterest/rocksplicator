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

import com.pinterest.rocksplicator.controller.tasks.TaskBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;

/**
 *
 * Thread pool executor service for executing Rocksplicator tasks.
 *
 * @author Shu Zhang (shu@pinterest.com)
 *
 */
public final class WorkerPool {

  // TODO: graceful shutdown.
  private static final Logger LOG = LoggerFactory.getLogger(WorkerPool.class);
  private final Semaphore idleWorkersSemaphore;
  private final ConcurrentHashMap<String, FutureTask> runningTasks;
  private final ExecutorService executorService;

  public WorkerPool(ExecutorService executorService, Semaphore idleWorkersSemaphore) {
    this.executorService = executorService;
    this.idleWorkersSemaphore = idleWorkersSemaphore;
    this.runningTasks = new ConcurrentHashMap<>();
  }

  /**
   * Assign a task to a worker thread.
   * @param task the task to execute
   * @throws Exception if there is a running task for the cluster.
   */
  public boolean assignTask(TaskBase task) throws Exception {
    if (runningTasks.containsKey(task.getCluster())) {
      String errorMessage =
          "Cannot add execute more than 1 task for the cluster " + task.getCluster();
      LOG.error(errorMessage);
      return false;
    }
    FutureTask futureTask = new FutureTask(task);
    runningTasks.put(task.getCluster(), futureTask);
    executorService.submit(() -> {
      try {
        futureTask.run();
      } finally {
        runningTasks.remove(task.getCluster());
        idleWorkersSemaphore.release();
      }
    });
    return true;
  }

  /**
   * Abort the running task on a cluster.
   * @param cluster the name of the cluster
   * @return
   */
  public boolean abortTask(String cluster) throws Exception {
    FutureTask runningTask = runningTasks.get(cluster);
    if (runningTask == null) {
      LOG.error("No running task of cluster " + cluster);
      return false;
    }
    // An Interrupted exception will be thrown to Task, and onFailure() will be triggered.'
    return runningTask.cancel(true);
  }

}
