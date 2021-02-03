package com.pinterest.rocksplicator.shardmap;

import java.util.Set;

public interface ShardMap {

  Set<String> getResources();

  ResourceMap getResourceMap(String resource);
}
