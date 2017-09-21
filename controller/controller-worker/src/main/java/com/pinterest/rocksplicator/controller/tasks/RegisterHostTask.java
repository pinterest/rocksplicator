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

import com.pinterest.rocksplicator.controller.ClusterManager;
import com.pinterest.rocksplicator.controller.bean.HostBean;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * The task uses ClusterManager to register a host to a cluster.
 *
 * @author shu (shu@pinterest.com)
 *
 */
public class RegisterHostTask extends AbstractTask<RegisterHostTask.Param> {

  private static final Logger LOG = LoggerFactory.getLogger(RegisterHostTask.class);

  @Inject
  private ClusterManager clusterManager;

  public RegisterHostTask(HostBean hostToRegister) {
    this(new Param().setHostToRegister(hostToRegister));
  }

  public RegisterHostTask(Param param) {
    super(param);
  }

  @Override
  public void process(Context ctx) throws Exception {
    boolean registerResult = clusterManager.registerToCluster(
        ctx.getCluster(), getParameter().getHostToRegister());
    if (!registerResult) {
      ctx.getTaskQueue().failTask(ctx.getId(), "Failed to register to cluster");
    } else {
      ctx.getTaskQueue().finishTask(ctx.getId(), "Succesfully registered");
    }
  }

  public static class Param extends Parameter {
    @JsonProperty
    private HostBean hostToRegister;

    public HostBean getHostToRegister() {
      return hostToRegister;
    }

    public Param setHostToRegister(HostBean hostToRegister) {
      this.hostToRegister = hostToRegister;
      return this;
    }
  }
}
