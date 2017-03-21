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

import com.pinterest.rocksplicator.controller.Task;

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
   * Returns a {@link Task} bean which represents the content of this task
   *
   * @return Task bean
   * @throws JsonProcessingException
   */
  public Task getBean() throws JsonProcessingException {
    Task task = new Task();
    task.name = getName();
    task.body = getParameter().serialize();
    task.priority = getPriority();
    return task;
  }

  /**
   * Subclasses implement this method for task logic.
   */
  public abstract void process(Context ctx) throws Exception;

}
