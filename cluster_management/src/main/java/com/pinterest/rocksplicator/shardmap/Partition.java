package com.pinterest.rocksplicator.shardmap;

import java.util.Objects;

public class Partition {

  private final String partitionName;

  public Partition(String resourceName, int partitionId) {
    this(String.format("%s_%d", resourceName, partitionId));
  }

  public Partition(String resourceName, String partitionStr) {
    this(String.format("%s_%d", resourceName, Integer.parseInt(partitionStr)));
  }

  public Partition(String partitionName) {
    this.partitionName = partitionName;
  }

  public String getPartitionName() {
    return this.partitionName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Partition partition = (Partition) o;
    return partitionName.equals(partition.partitionName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(partitionName);
  }

  @Override
  public String toString() {
    return "Partition{" +
        "partitionName='" + partitionName + '\'' +
        '}';
  }
}
