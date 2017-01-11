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

import com.pinterest.rocksplicator.controller.bean.ClusterBean;

import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;
import java.util.Optional;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
@Path("/v1/clusters")
public class Clusters {

  /**
   * Retrieves cluster information by cluster name.
   *
   * @param clusterName name of the cluster
   * @return            ClusterBean
   */
  @GET
  @Path("/{clusterName : [a-zA-Z0-9\\-_]+}")
  public ClusterBean get(@PathParam("clusterName") String clusterName) {
    throw new UnsupportedOperationException("method not implemented.");
  }

  /**
   * Gets all clusters managed by the controller.
   *
   * @return a list of {@link ClusterBean}
   */
  @GET
  public List<ClusterBean> getAll() {
    throw new UnsupportedOperationException("method not implemented.");
  }

  /**
   * Initializes a given cluster. This may include adding designated tag
   * in DB and/or writing shard config to zookeeper.
   *
   * @param clusterName name of the cluster
   */
  @POST
  @Path("/initialize/{clusterName : [a-zA-Z0-9\\-_]+}")
  public void initialize(@PathParam("clusterName") String clusterName) {
    throw new UnsupportedOperationException("method not implemented.");
  }

  /**
   * Replaces a host in a given cluster with a new one. If new host is provided
   * in the query parameter, that host will be used to replace the old one.
   * Otherwise, controller will randomly pick one for the user.
   *
   *
   * @param clusterName name of the cluster
   * @param oldHost     host to be replaced, in the format of ip:port
   * @param newHost     (optional) new host to add, in the format of ip:port
   */
  @POST
  @Path("/replaceHost/{clusterName : [a-zA-Z0-9\\-_]+}")
  public void replaceHost(@PathParam("clusterName") String clusterName,
                          @NotEmpty @QueryParam("oldHost") String oldHost,
                          @QueryParam("newHost") Optional<String> newHost) {
    throw new UnsupportedOperationException("method not implemented.");
  }

  /**
   * Loads a dataset into a given cluster.
   *
   * @param clusterName name of the cluster
   * @param dataSetName name of the dataset
   * @param dataPath    data path on S3
   */
  @POST
  @Path("/loadData/{clusterName : [a-zA-Z0-9\\-_]+}")
  public void loadData(@PathParam("clusterName") String clusterName,
                       @NotEmpty @QueryParam("dataSetName") String dataSetName,
                       @NotEmpty @QueryParam("dataPath") String dataPath) {
    throw new UnsupportedOperationException("method not implemented.");
  }

  /**
   * Locks a given cluster. Outside system may use this API to synchronize
   * operations on the same cluster. It is caller's responsibility to properly
   * release the lock via {@link #unlock(String)}.
   *
   * @param clusterName name of the cluster to lock
   */
  @POST
  @Path("/lock/{clusterName : [a-zA-Z0-9\\-_]+}")
  public void lock(@PathParam("clusterName") String clusterName) {
    throw new UnsupportedOperationException("method not implemented.");
  }

  /**
   * Unlocks a given cluster.
   *
   * @param clusterName name of the cluster to unlock
   */
  @POST
  @Path("/unlock/{clusterName : [a-zA-Z0-9\\-_]+}")
  public void unlock(@PathParam("clusterName") String clusterName) {
    throw new UnsupportedOperationException("method not implemented.");
  }
}
