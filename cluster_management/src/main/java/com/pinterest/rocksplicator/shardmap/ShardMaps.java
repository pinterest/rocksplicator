package com.pinterest.rocksplicator.shardmap;

import org.json.simple.JSONObject;

public class ShardMaps {

  public static ShardMap fromJson(JSONObject shardMapJsonObject) {
    return new JsonShardMap(shardMapJsonObject);
  }
}
