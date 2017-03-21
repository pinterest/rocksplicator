/*
 *  Copyright 2017 Pinterest, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.pinterest.rocksplicator.controller.tasks;

import com.pinterest.rocksplicator.controller.TaskQueue;


/**
 * Runtime context needed to drive {@link TaskBase}
 *
 * @author Ang Xu (angxu@pinterest.com)
 */
public class Context {

  private long id;
  private String cluster;
  private String worker;
  private TaskQueue taskQueue;

  public Context(long id, String cluster, TaskQueue taskQueue, String worker) {
    this.id = id;
    this.cluster = cluster;
    this.worker = worker;
    this.taskQueue = taskQueue;
  }

  public long getId() {
    return id;
  }

  public String getCluster() {
    return cluster;
  }

  public String getWorker() {
    return worker;
  }

  public TaskQueue getTaskQueue() {
    return taskQueue;
  }

}
