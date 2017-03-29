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

package com.pinterest.rocksplicator.controller.tasks;

import com.pinterest.rocksplicator.controller.TaskEntity;

import com.fasterxml.jackson.core.JsonProcessingException;


/**
 * Task interface.
 *
 * @author Shu Zhang (shu@pinterest.com)
 */
public abstract class TaskBase<PARAM extends Parameter> {

  public static int RESERVED_PRIORITY = 0;
  public static int HIGH_PRIORITY = 1;
  public static int DEFAULT_PRIORITY = 2;

  private final PARAM param;

  public TaskBase(PARAM param) {
    this.param = param;
  }

  /**
   * Returns fully qualified class name of this task
   *
   * @return name of the task
   */
  public String getName() {
    return getClass().getName();
  }

  /**
   * Returns the priority of this task
   *
   * @return priority
   */
  public int getPriority() {
    return DEFAULT_PRIORITY;
  }

  /**
   * Returns the parameter of this task
   *
   * @return parameter
   */
  public PARAM getParameter() {
    return param;
  }

  /**
   * Returns a {@link TaskEntity} which represents the content of this task
   *
   * @return TaskEntity entity of the task
   * @throws JsonProcessingException
   */
  public TaskEntity getEntity() throws JsonProcessingException {
    TaskEntity taskEntity = new TaskEntity();
    taskEntity.name = getName();
    taskEntity.body = getParameter().serialize();
    taskEntity.priority = getPriority();
    return taskEntity;
  }

  /**
   * Subclasses implement this method for task logic.
   */
  public abstract void process(Context ctx) throws Exception;

  /**
   * Returns a chained task of this task and {@code nextTask} in a way that if this task returns
   * successfully, the nextTask will be processed immediately after this task.
   * @param nextTask
   * @return a chained task
   * @throws JsonProcessingException
   */
  public TaskBase andThen(TaskBase nextTask) throws JsonProcessingException {
    return new ChainedTask(
        new ChainedTask.Param()
            .setT1(this.getEntity())
            .setT2(nextTask.getEntity())
    );
  }

  /**
   * Returns a retry task that will retry this task up to {@code maxRetry} times.
   * @param maxRetry max number of retries
   * @return a retry task
   * @throws JsonProcessingException
   */
  public TaskBase retry(int maxRetry) throws JsonProcessingException {
    return new RetryTask(
        new RetryTask.Param()
            .setTask(this.getEntity())
            .setMaxRetry(maxRetry)
    );
  }
}
