package com.pinterest.rocksplicator.shardmap;

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
}
