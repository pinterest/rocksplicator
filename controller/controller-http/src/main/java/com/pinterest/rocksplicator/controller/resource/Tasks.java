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

package com.pinterest.rocksplicator.controller.resource;

import com.pinterest.rocksplicator.controller.Cluster;
import com.pinterest.rocksplicator.controller.Task;
import com.pinterest.rocksplicator.controller.TaskQueue;
import com.pinterest.rocksplicator.controller.bean.TaskState;
import com.pinterest.rocksplicator.controller.Utils;

import com.google.common.collect.ImmutableMap;
import org.eclipse.jetty.http.HttpStatus;

import java.util.List;
import java.util.Optional;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
@Path("/v1/tasks")
public class Tasks {

  private final TaskQueue taskQueue;

  public Tasks(TaskQueue taskQueue) {
    this.taskQueue = taskQueue;
  }

  /**
   * Retrieves a task by the task id.
   *
   * @param id  task id
   * @return    {@link Task} or null if task doesn't exist
   */
  @GET
  @Path("/{id : [0-9]+}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response get(@PathParam("id") Long id) {
    Task result = taskQueue.findTask(id);
    if (result == null) {
      String message = String.format("Task %s cannot be found", id);
      return Utils.buildResponse(HttpStatus.NOT_FOUND_404, ImmutableMap.of("message", message));
    } else {
      return Utils.buildResponse(HttpStatus.OK_200, result);
    }
  }

  /**
   * Retrieves all the tasks that match the given cluster namespace/name and/or
   * the {@link TaskState state}.
   *
   * @param clusterName name of the cluster being queried
   * @param state       state of the task being queried.
   * @return            a list of {@link Task}s
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response findTasks(@QueryParam("namespace") Optional<String> namespace,
                            @QueryParam("clusterName") Optional<String> clusterName,
                            @QueryParam("state") Optional<TaskState> state) {
    if(!namespace.isPresent() && namespace.isPresent()) {
      Utils.buildResponse(HttpStatus.NOT_FOUND_404,
          ImmutableMap.of("message", "we don't allow empty namespace with non-empty cluster name"));
    }
    List<Task> result = taskQueue.peekTasks(new Cluster(namespace.orElse(""), clusterName.orElse("")),
                                            state.map(TaskState::intValue).orElse(null));
    return Utils.buildResponse(HttpStatus.OK_200, result);
  }

  /**
   * Remove a task by its id
   *
   * @param id id of the task being removed
   * @return Response object
   */
  @DELETE
  @Path("/{id : [0-9]+}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response removeTask(@PathParam("id") Long id) {
    if (taskQueue.removeTask(id)) {
      return Utils.buildResponse(HttpStatus.OK_200, true);
    } else {
      String message = String.format("Cannot find task with id %s", id);
      return Utils.buildResponse(HttpStatus.NOT_FOUND_404, ImmutableMap.of("message", message));
    }
  }

}
