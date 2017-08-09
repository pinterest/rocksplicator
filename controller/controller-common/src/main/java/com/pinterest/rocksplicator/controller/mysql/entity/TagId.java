package com.pinterest.rocksplicator.controller.mysql.entity;

import com.pinterest.rocksplicator.controller.Cluster;

/**
 * Composite Primary key for Tag table
 */
public class TagId {
  String namespace;
  String name;

  public TagId(Cluster cluster) {
    this.namespace = cluster.getNamespace();
    this.name = cluster.getName();
  }
}
