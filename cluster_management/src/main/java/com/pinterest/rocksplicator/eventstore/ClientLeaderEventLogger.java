package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.shardmap.ShardMap;

import java.io.Closeable;

public interface ClientLeaderEventLogger extends Closeable {
  void process(ShardMap shardMap, long shardMapNotificationTimeMillis);
}
