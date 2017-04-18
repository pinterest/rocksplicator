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

import com.pinterest.rocksplicator.controller.tasks.Context;
import com.pinterest.rocksplicator.controller.tasks.AbstractTask;
import com.pinterest.rocksplicator.controller.tasks.TaskFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

/**
 *
 * The worker pool contains an executor service for executing Rocksplicator tasks.
 *
 * @author Shu Zhang (shu@pinterest.com)
 *
 */
public final class WorkerPool {

  // TODO: graceful shutdown.
  private static final Logger LOG = LoggerFactory.getLogger(WorkerPool.class);
  private final Semaphore idleWorkersSemaphore;
  private final ConcurrentHashMap<String, Future> runningTasks;
  private final ExecutorService executorService;
  private final TaskQueue taskQueue;

  public WorkerPool(ExecutorService executorService,
                    Semaphore idleWorkersSemaphore,
                    TaskQueue taskQueue) {
    this.executorService = executorService;
    this.idleWorkersSemaphore = idleWorkersSemaphore;
    this.runningTasks = new ConcurrentHashMap<>();
    this.taskQueue = taskQueue;
  }

  /**
   * Assign a task to a worker thread.
   * @param task the task to execute
   * @throws Exception if there is a running task for the cluster.
   */
  public synchronized boolean assignTask(Task task) throws Exception {
    if (task == null) {
      LOG.error("The task is null, cannot be assigned");
      return false;
    }
    AbstractTask baseTask = TaskFactory.getWorkerTask(task);

    Future<?> future = executorService.submit(() -> {
      try {
        final Context ctx = new Context(task.id, task.clusterName, taskQueue,
            WorkerConfig.getHostName() + ":" + Thread.currentThread().getName());
        baseTask.process(ctx);
        LOG.info("Finished processing task {}.", task.name);
      } catch (Throwable t) {
        LOG.error("Unexpected exception thrown from task {}.", task, t);
        // This is just a defensive maneuver. It's okay if the task has already been
        // completed by itself. Therefore, the result of this operation is ignored.
        taskQueue.failTask(task.id, t.getMessage());
      } finally {
        runningTasks.remove(task.clusterName);
        idleWorkersSemaphore.release();
      }
    });

    runningTasks.put(task.clusterName, future);
    return true;
  }

  /**
   * Abort the running task on a cluster.
   * @param cluster the name of the cluster
   * @return
   */
  public synchronized boolean abortTask(String cluster) throws Exception {
    Future runningTask = runningTasks.get(cluster);
    if (runningTask == null) {
      LOG.error("No running task of cluster " + cluster);
      return false;
    }
    // An Interrupted exception will be thrown to TaskEntity, and onFailure() will be triggered.
    return runningTask.cancel(true);
  }

}
