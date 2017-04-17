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

import com.pinterest.rocksplicator.controller.TaskBase;

import com.google.inject.Injector;
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
  private static volatile Injector INJECTOR;

  public static void setInjector(Injector injector) {
    INJECTOR = injector;
  }

  public static com.pinterest.rocksplicator.controller.tasks.TaskBase getWorkerTask(TaskBase task) {
    try {
      Class<com.pinterest.rocksplicator.controller.tasks.TaskBase> taskClazz = loadTaskClass(task.name);
      Class<Parameter> paramClazz = Generics.getTypeParameter(taskClazz, Parameter.class);
      Parameter parameter = Parameter.deserialize(task.body, paramClazz);
      com.pinterest.rocksplicator.controller.tasks.TaskBase taskBase = taskClazz.getConstructor(paramClazz).newInstance(parameter);
      Injector injector = INJECTOR;
      if (injector != null) {
        injector.injectMembers(taskBase);
      }
      return taskBase;
    } catch (Exception e) {
      LOG.error("Cannot instantiate the implementation of " + task.name, e);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public static Class<com.pinterest.rocksplicator.controller.tasks.TaskBase> loadTaskClass(String taskName) throws ClassNotFoundException {
    Class clazz = Thread.currentThread().getContextClassLoader().loadClass(taskName);
    return clazz;
  }
}
