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

import com.pinterest.rocksplicator.controller.util.AdminClientFactory;
import com.pinterest.rocksplicator.controller.util.EmailSender;

import com.google.inject.AbstractModule;
import org.apache.curator.framework.CuratorFramework;

/**
 * This class manages dependencies need being injected into Tasks.
 *
 * @author Ang Xu (angxu@pinterest.com)
 */
public class TaskModule extends AbstractModule {

  private final CuratorFramework zkClient;
  private final AdminClientFactory adminClientFactory;
  private final EmailSender emailSender;

  public TaskModule(CuratorFramework zkClient,
                    AdminClientFactory adminClientFactory,
                    EmailSender emailSender) {
    this.zkClient = zkClient;
    this.adminClientFactory = adminClientFactory;
    this.emailSender = emailSender;
  }

  @Override
  protected void configure() {
    bind(CuratorFramework.class).toInstance(zkClient);
    bind(AdminClientFactory.class).toInstance(adminClientFactory);
  }
}
