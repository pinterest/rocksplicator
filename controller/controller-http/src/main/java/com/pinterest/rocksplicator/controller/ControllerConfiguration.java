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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * @author Ang Xu (angxu@pinterest.com)
 */
public class ControllerConfiguration extends Configuration {

  @NotEmpty
  private String zkHostsFile;

  @NotEmpty
  private String zkCluster;

  @NotEmpty
  private String defaultNamespace = "rocksdb";

  @NotEmpty
  private String jdbcUrl;

  @NotEmpty
  private String mysqlUser;

  private String mysqlPassword = "";

  @JsonProperty
  public String getZkHostsFile() {
    return zkHostsFile;
  }

  @JsonProperty
  public void setZkHostsFile(String zkHostsFile) {
    this.zkHostsFile = zkHostsFile;
  }

  @JsonProperty
  public String getDefaultNamespace() {
    return defaultNamespace;
  }

  @JsonProperty
  public void setDefaultNamespace(String defaultNamespace) {
    this.defaultNamespace = defaultNamespace;
  }

  @JsonProperty
  public String getZkCluster() {
    return zkCluster;
  }

  @JsonProperty
  public void setZkCluster(String zkCluster) {
    this.zkCluster = zkCluster;
  }

  @JsonProperty
  public String getJdbcUrl() {
    return jdbcUrl;
  }

  @JsonProperty
  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  @JsonProperty
  public String getMysqlUser() {
    return mysqlUser;
  }

  @JsonProperty
  public void setMysqlUser(String mysqlUser) {
    this.mysqlUser = mysqlUser;
  }

  @JsonProperty
  public String getMysqlPassword() {
    return mysqlPassword;
  }

  public void setMysqlPassword(String mysqlPassword) {
    this.mysqlPassword = mysqlPassword;
  }

}
