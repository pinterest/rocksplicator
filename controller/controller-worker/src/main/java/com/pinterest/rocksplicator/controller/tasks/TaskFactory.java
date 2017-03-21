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

import io.dropwizard.util.Generics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The factory that produces worker task objects basing on the Task POJO from task-queue.
 *
 * @author Shu Zhang (shu@pinterest.com)
 *
 */
public final class TaskFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TaskFactory.class);

  public static TaskBase getWorkerTask(TaskEntity task) {
    try {
      Class<TaskBase> taskClazz = loadTaskClass(task.name);
      Class<Parameter> paramClazz = Generics.getTypeParameter(taskClazz, Parameter.class);
      Parameter parameter = Parameter.deserialize(task.body, paramClazz);
      return taskClazz.getConstructor(paramClazz).newInstance(parameter);
    } catch (Exception e) {
      LOG.error("Cannot instantiate the implementation of " + task.name, e);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public static Class<TaskBase> loadTaskClass(String taskName) throws ClassNotFoundException {
    Class clazz = Thread.currentThread().getContextClassLoader().loadClass(taskName);
    return clazz;
  }
}
