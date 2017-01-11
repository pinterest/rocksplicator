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

import com.pinterest.rocksplicator.controller.bean.TaskBean;

import java.util.List;
import java.util.Optional;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
@Path("/v1/tasks")
public class Tasks {

  /**
   * Retrieves a task by the task id.
   *
   * @param id  task id
   * @return    {@link TaskBean} or null if task doesn't exist
   */
  @GET
  @Path("/{id : [0-9]+}")
  @Produces(MediaType.APPLICATION_JSON)
  public TaskBean get(@PathParam("id") Long id) {
    throw new UnsupportedOperationException("method not implemented");
  }

  /**
   * Retrieves all the tasks that match the given cluster name and/or
   * the {@link com.pinterest.rocksplicator.controller.bean.TaskBean.State state}.
   *
   * @param clusterName name of the cluster being queried
   * @param state       state of the task being queried.
   * @return            a list of {@link TaskBean}s
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public List<TaskBean> findTasks(@QueryParam("clusterName") Optional<String> clusterName,
                                  @QueryParam("state") Optional<TaskBean.State> state) {
    throw new UnsupportedOperationException("method not implemented");
  }

}
