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

import com.google.common.collect.ImmutableMap;
import com.pinterest.rocksplicator.controller.Task;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The factory that produces worker task objects basing on the Task POJO from task-queue.
 *
 * @author Shu Zhang (shu@pinterest.com)
 *
 */
public final class TaskFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TaskFactory.class);
  private static final Map<String, Class> tasks;

  static {
    // Initialize all supported tasks.
    tasks = ImmutableMap.of(
      "SleepIncrementTask", SleepIncrementTask.class
    );
  }

  public static TaskBase getWorkerTask(Task task) {
    if (tasks.get(task.name) == null) {
      LOG.error(task.name + " doesn't have implementation");
      return null;
    }
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonNode taskBody = mapper.readTree(task.body);
      TaskBase workerTask = (TaskBase) tasks.get(task.name).getConstructor(
          long.class, String.class, JsonNode.class).newInstance(
          task.id, task.clusterName, taskBody);
      return workerTask;
    } catch (Exception e) {
      LOG.error("Cannot instantiate the implementation of " + task.name, e);
      return null;
    }
  }
}
