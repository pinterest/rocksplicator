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

package com.pinterest.rocksplicator.controller;

import java.sql.Timestamp;
import java.util.Date;

/**
 * A {@link Task} is a POJO contains both {@link TaskBase} and the metadata of this task.
 * It can be retrieved from {@TaskQueue}.
 *
 * @author Bo Liu (bol@pinterest.com)
 */
public class Task extends TaskBase {

  public enum TaskState {
    PENDING,
    RUNNING,
    DONE,
    FAILED;
  }

  public long id;
  public int state;
  public String name;
  public String clusterName;
  public Date createdAt;
  public Date runAfter;
  public Date lastAliveAt;
  public String claimedWorker;
  public String output;

  public Task setId(long id) {
    this.id = id;
    return this;
  }

  public Task setState(int state) {
    this.state = state;
    return this;
  }

  public Task setClusterName(String clusterName) {
    this.clusterName = clusterName;
    return this;
  }

  public Task setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
    return this;
  }

  public Task setRunAfter(Date runAfter) {
    this.runAfter = runAfter;
    return this;
  }

  public Task setLastAliveAt(Date lastAliveAt) {
    this.lastAliveAt = lastAliveAt;
    return this;
  }

  public Task setClaimedWorker(String claimedWorker) {
    this.claimedWorker = claimedWorker;
    return this;
  }

  public Task setOutput(String output) {
    this.output = output;
    return this;
  }

  public Task setName(String name) {
    this.name = name;
    return this;
  }

  public Task setPriority(int priority) {
    this.priority = priority;
    return this;
  }

  public Task setBody(String body) {
    this.body = body;
    return this;
  }

  public Task(TaskBase t) {
    super.name = t.name;
    super.body = t.body;
    super.priority = t.priority;
  }

  public Task() {}
}
