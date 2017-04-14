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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * @author Bo Liu (bol@pinterest.com)
 */
public interface TaskQueue {

  /**
   * Create a new cluster in the task queue.
   * @param clusterName
   * @return false on error
   */
  default boolean createCluster(final String clusterName) {
    return true;
  }

  /**
   * Enqueue a task.
   * @param taskEntity entity of the task to enqueue
   * @param clusterName Which cluster is this task for
   * @param runDelaySeconds the task should be delayed for this many of seconds to run. If <= 0, no
   *                        delay required
   * @return false on error
   */
  default boolean enqueueTask(final TaskEntity taskEntity,
                             final String clusterName,
                             final int runDelaySeconds) {
    return true;
  }

  /**
   * Dequeue the task with highest priority (least priority #) and 1) which is currently in pending
   * state; 2) whose cluster is not locked; 3) whose runDelaySeconds has passed.
   * This function will atomically lock the cluster associated with it.
   * @param worker the worker who is calling this function
   * @return the dequeued task, or null if no eligible task found
   */
  default Task dequeueTask(final String worker) {
    return new Task(new TaskEntity());
  }

  /**
   * Ack the task queue that the task has finished.
   * It will also atomically unlock the cluster the task associated with.
   * @param id which task to finish
   * @param output output of this task
   * @return false on error
   */
  default boolean finishTask(final long id, final String output) {
    return true;
  }

  /**
   * Ack the task queue that the task has failed.
   * It will also atomically unlock the cluster the task associated with.
   * @param id which task to fail
   * @param reason output of this task
   * @return false on error
   */
  default boolean failTask(final long id, final String reason) {
    return true;
  }

  /**
   * Atomically finish a task, and enqueue a new task associated with the same cluster as task(id).
   * The newly enqueued task if of priority 0 and in running state.
   * @param id which task to finish
   * @param output output of the finished task
   * @param newTaskEntity the task entity of the new task
   * @param worker the worker who is calling this function
   * @return the task id for the newly enqueued task on success, -1 on error
   */
  default long finishTaskAndEnqueueRunningTask(final long id,
                                               final String output,
                                               final TaskEntity newTaskEntity,
                                               final String worker) {
    return 0;
  }

  /**
   * Atomically ack the task queue that the task has finished, and enqueue a new task.
   * @param id which task to finish
   * @param output output of the finished task
   * @param taskEntity the task entity of the task to enqueue
   * @param runDelaySeconds the run delay seconds for the task to enqueue
   * @return false on error
   */
  default boolean finishTaskAndEnqueuePendingTask(final long id,
                                                  final String output,
                                                  final TaskEntity taskEntity,
                                                  final int runDelaySeconds) {
    return true;
  }

  /**
   * Lock a cluster
   * @param cluster which cluster to lock
   * @return false on error
   */
  default boolean lockCluster(final String cluster) {
    return true;
  }

  /**
   * Unlock a cluster
   * @param cluster which cluster to unlock
   * @return false on error
   */
  default boolean unlockCluster(final String cluster) {
    return true;
  }

  /**
   * Atomically remove an existing cluster and all its associated tasks from the queue.
   * @precondition the cluster must be in unlock state. otherwise, the call will fail.
   * @param cluster which cluster to remove
   * @return false on error
   */
  default boolean removeCluster(final String cluster) {
    return true;
  }

  /**
   * Remove finished tasks.
   * @return the number of finished tasks which are removed by this call.
   */
  default int removeFinishedTasks() {
    return 0;
  }

  /**
   * Reset all zombie tasks to pending state, and unlock the clusters associated with them.
   * A zombie task is a task in running state whose last known alive time is more than a threshold.
   * @return the number of zombie tasks which are rest by this call
   */
  default int resetZombieTasks(final int zombieThresholdSeconds) {
    return 0;
  }

  /**
   * Ack the task queue that the task is still alive, and running.
   * @param id which task to keep alive
   * @return false on error
   */
  default boolean keepTaskAlive(final long id) {
    return true;
  }

  /**
   * Return all tasks in the state and associated with clusterName.
   * If clusterName is null, all clusters are included.
   * If state is null, all states are included.
   * @param clusterName which cluster to peek tasks for
   * @param state peek tasks in this state only
   * @return the list of tasks found
   */
  default List<Task> peekTasks(final String clusterName,
                              final Integer state) {
    return new ArrayList<>();
  }

  /**
   * Find task by its id.
   * @param id id of the task
   * @return task or null
   */
  default Task findTask(long id) {
    return new Task(new TaskEntity());
  }

  /**
   * Return all clusters managed by this task queue.
   * @return a set of cluster names
   */
  default Set<String> getAllClusters() {
    return Collections.emptySet();
  }
}
