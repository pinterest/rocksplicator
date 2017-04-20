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
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.validation.constraints.NotNull;
import java.util.Date;

/**
 * MySQL tag table schema:
 *  name VARCHAR(128) NOT NULL,
 *  locks TINYINT UNSIGNED NOT NULL,
 *  created_at DATETIME NOT NULL,
 *  owner VARCHAR(256),
 *  PRIMARY KEY (name)
 */

@Entity(name = "tag")
@Table(name = "tag")
@NamedQueries({
    @NamedQuery(name = "tag.findAll",
        query = "SELECT t.name FROM tag t"),
})
public class TagEntity {

  @Id
  @Column(name = "name")
  @NotNull
  private String name;

  @Column(name = "locks")
  @NotNull
  private int locks;

  @Column(name = "created_at")
  @Temporal(TemporalType.TIMESTAMP)
  @NotNull
  private Date createdAt;

  @Column(name = "owner")
  private String owner;

  public TagEntity() {
    this.createdAt = new Date();
  }

  public String getName() {
    return name;
  }

  public TagEntity setName(String name) {
    this.name = name;
    return this;
  }

  public TagEntity setOwner(String owner) {
    this.owner = owner;
    return this;
  }

  public TagEntity setLocks(int locks) {
    this.locks = locks;
    return this;
  }

  public TagEntity setCreatedAt(Date createdAt) {
    this.createdAt = createdAt;
    return this;
  }

  public int getLocks() {
    return locks;
  }

  public Date getCreatedAt() {
    return createdAt;
  }

  public String getOwner() {
    return owner;
  }
}