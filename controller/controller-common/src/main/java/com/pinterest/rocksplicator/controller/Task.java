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

/**
 * A {@link Task} is a POJO contains both {@link TaskEntity} and the metadata of this task.
 * It can be retrieved from {@TaskQueue}.
 *
 * @author Bo Liu (bol@pinterest.com)
 */
public class Task extends TaskEntity {
  public long id;
  public int state;
  public String clusterName;
  public Timestamp createdAt;
  public Timestamp runAfter;
  public Timestamp lastAliveAt;
  public String claimedWorker;
  public String output;

  public Task(TaskEntity t) {
    super.name = t.name;
    super.body = t.body;
    super.priority = t.priority;
  }
}
