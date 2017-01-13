/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

/**
 * Represents the result of task execution.
 *
 * @author Shu Zhang (shu@pinterest.com)
 */
public class TaskExecutionResponse {

  private String response;
  private long duration;

  public long getDuration() {
    return duration;
  }

  public void setDuration(long duration) {
    this.duration = duration;
  }

  public String getResponse() {
    return response;
  }

  public void setResponse(String response) {
    this.response = response;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TaskExecutionResponse that = (TaskExecutionResponse) o;

    if (duration != that.duration) return false;
    return response != null ? response.equals(that.response) : that.response == null;

  }

  @Override
  public int hashCode() {
    int result = response != null ? response.hashCode() : 0;
    result = 31 * result + (int) (duration ^ (duration >>> 32));
    return result;
  }
}
