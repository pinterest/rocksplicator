package com.pinterest.rocksplicator.utils;

public class ZkPathUtils {
  private static final String BASE_PATH_PER_RESOURCE_SHARD_MAP
      = "/rocksplicator-shard_map/gzipped-json/byResource";

  public static String getClusterShardMapParentPath(String clusterName) {
    return String.format("%s/%s", BASE_PATH_PER_RESOURCE_SHARD_MAP, clusterName);
  }
  public static String getClusterResourceShardMapPath(String clusterName, String resourceName) {
    return String.format("%s/%s/%s", BASE_PATH_PER_RESOURCE_SHARD_MAP, clusterName, resourceName);
  }
}
