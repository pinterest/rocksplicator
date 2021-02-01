package com.pinterest.rocksplicator.shardmap;

import java.util.List;
import java.util.Set;
import javax.swing.Painter;

public interface ResourceMap {

  String getResource();

  int getNumShards();

  Set<Instance> getInstances();

  Set<Partition> getAllKnownPartitions();

  Set<Partition> getAllMissingPartitions();

  Set<Partition> getAllPartitions();

  List<Replica> getAllReplicasForPartition(Partition partition);

  List<Replica> getAllReplicasOnInstance(Instance instance);
}
