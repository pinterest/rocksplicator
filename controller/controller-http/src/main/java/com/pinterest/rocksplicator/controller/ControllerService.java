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

import com.pinterest.rocksplicator.controller.resource.Clusters;
import com.pinterest.rocksplicator.controller.resource.Tasks;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

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
    environment.jersey().register(new Clusters());
    environment.jersey().register(new Tasks());

  }

  public static void main(String[] args) throws Exception {
    new ControllerService().run(args);
  }
}
