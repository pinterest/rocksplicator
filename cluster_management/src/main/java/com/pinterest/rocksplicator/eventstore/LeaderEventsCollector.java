package com.pinterest.rocksplicator.eventstore;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;

public interface LeaderEventsCollector {

  LeaderEventsCollector addEvent(LeaderEventType eventType, String leaderNode);

  LeaderEventsCollector addEvent(LeaderEventType eventType, String leaderNode, long eventTimeMillis);

  void commit();
}
