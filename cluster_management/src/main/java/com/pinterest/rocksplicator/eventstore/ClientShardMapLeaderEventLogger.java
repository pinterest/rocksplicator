package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.shardmap.ShardMap;

import java.io.Closeable;

public interface ClientShardMapLeaderEventLogger extends Closeable {
  void process(ShardMap shardMap, long shardMapNotificationTimeMillis);
}
