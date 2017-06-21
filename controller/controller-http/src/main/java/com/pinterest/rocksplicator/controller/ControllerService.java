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

package com.pinterest.rocksplicator.controller;

import com.pinterest.rocksplicator.controller.mysql.MySQLTaskQueue;
import com.pinterest.rocksplicator.controller.resource.Clusters;
import com.pinterest.rocksplicator.controller.resource.Tasks;

import com.pinterest.rocksplicator.controller.util.ZookeeperConfigParser;
import io.dropwizard.Application;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Environment;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.retry.RetryOneTime;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class ControllerService extends Application<ControllerConfiguration> {

  @Override
  public String getName() {
    return "rocksplicator-controller";
  }

  @Override
  public void run(ControllerConfiguration configuration,
                  Environment environment) throws Exception {
    TaskQueue taskQueue = new MySQLTaskQueue(configuration.getJdbcUrl(), configuration
        .getMysqlUser(), configuration.getMysqlPassword());
    CuratorFramework zkClient = CuratorFrameworkFactory.newClient(
        ZookeeperConfigParser.parseEndpoints(
            configuration.getZkHostsFile(), configuration.getZkCluster()),
        new RetryOneTime(3000));

    environment.lifecycle().manage(new Managed() {
      @Override
      public void start() throws Exception {
        zkClient.start();
      }

      @Override
      public void stop() throws Exception {
        zkClient.close();
      }
    });

    environment.jersey().register(
        new Clusters(configuration.getZkPath(), zkClient, taskQueue)
    );
    environment.jersey().register(new Tasks(taskQueue));

  }

  public static void main(String[] args) throws Exception {
    new ControllerService().run(args);
  }
}
