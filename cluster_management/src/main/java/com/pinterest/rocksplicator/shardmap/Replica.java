package com.pinterest.rocksplicator.shardmap;

import java.util.Objects;

public class Replica {

  private final Instance instance;
  private final Partition partition;
  private final ReplicaState replicaState;

  public Replica(Partition partition, Instance instance, ReplicaState replicaState) {
    this.partition = partition;
    this.instance = instance;
    this.replicaState = replicaState;
  }

  public ReplicaState getReplicaState() {
    return replicaState;
  }

  public Instance getInstance() {
    return instance;
  }

  public Partition getPartition() {
    return partition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Replica replica = (Replica) o;
    return instance.equals(replica.instance) &&
        partition.equals(replica.partition) &&
        replicaState == replica.replicaState;
  }

  @Override
  public int hashCode() {
    return Objects.hash(instance, partition, replicaState);
  }

  @Override
  public String toString() {
    return "Replica{" +
        "instance=" + instance +
        ", partition=" + partition +
        ", replicaState=" + replicaState +
        '}';
  }
}
