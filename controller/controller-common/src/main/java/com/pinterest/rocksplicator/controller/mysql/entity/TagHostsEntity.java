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

package com.pinterest.rocksplicator.controller.mysql.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.JoinColumn;
import javax.persistence.JoinColumns;
import javax.persistence.ManyToOne;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

/**
 *
 * MySQL tag_hosts table schema:
 *  namespace VARCHAR(128) NOT NULL,
 *  name VARCHAR(128) NOT NULL,
 *  live_hosts MEDIUMTEXT,
 *  blacklisted_hosts MEDIUMTEXT,
 *  FOREIGN KEY (namespace, name) REFERENCES tag(namespace, name) ON UPDATE RESTRICT ON DELETE CASCADE
 *
 * @author shu (shu@pinterest.com)
 */
@Entity(name = "tag_hosts")
@Table(name = "tag_hosts")
@NamedQueries({
    @NamedQuery(name = "tag_hosts.getCluster",
        query = "SELECT t FROM tag_hosts t INNER JOIN t.cluster c " +
            "WHERE c.name = :name AND c.namespace = :namespace"),
})
public class TagHostsEntity {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id")
  private long id;

  @ManyToOne
  @JoinColumns({
      @JoinColumn(name="tag_namespace", referencedColumnName="namespace"),
      @JoinColumn(name="tag_name", referencedColumnName="name")
  })
  @NotNull
  private TagEntity cluster;

  @Column(name = "live_hosts")
  private String liveHosts;

  @Column(name = "blacklisted_hosts")
  private String blacklistedHosts;


  public TagEntity getCluster() {
    return cluster;
  }

  public TagHostsEntity setCluster(TagEntity cluster) {
    this.cluster = cluster;
    return this;
  }

  public String getLiveHosts() {
    return liveHosts;
  }

  public TagHostsEntity setLiveHosts(String liveHosts) {
    this.liveHosts = liveHosts;
    return this;
  }

  public String getBlacklistedHosts() {
    return blacklistedHosts;
  }

  public TagHostsEntity setBlacklistedHosts(String blacklistedHosts) {
    this.blacklistedHosts = blacklistedHosts;
    return this;
  }

  public long getId() { return id; }
}
