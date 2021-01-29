/// Copyright 2021 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author Gopal Rajpurohit (grajpurohit@pinterest.com)
//

package com.pinterest.rocksplicator.eventstore;

import static org.junit.Assert.assertEquals;

import com.pinterest.rocksplicator.codecs.Codec;
import com.pinterest.rocksplicator.codecs.ThriftStringEncoder;
import com.pinterest.rocksplicator.codecs.WrappedDataThriftCodec;
import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEvent;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventType;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

public class ZkMergeableEventStoreTest {

  private static final String CLUSTER_NAME = "myCluster";
  private static final String RESOURCE_NAME = "myResource";
  private static final String PARTITION_NAME = "myResource_0";
  private TestingServer zkTestServer;
  private CuratorFramework zkClient;
  private ZkMergeableEventStore<LeaderEventsHistory, LeaderEvent> zkStore;
  private Codec<LeaderEventsHistory, byte[]>
      codec =
      new WrappedDataThriftCodec(LeaderEventsHistory.class, SerializationProtocol.COMPACT,
          CompressionAlgorithm.GZIP);


  @Before
  public void setUp() throws Exception {
    zkTestServer = new TestingServer(-1);
    zkClient =
        CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(2000));
    zkClient.start();
    this.zkStore = new ZkMergeableEventStore(
        zkClient,
        CLUSTER_NAME,
        RESOURCE_NAME,
        PARTITION_NAME,
        codec,
        LeaderEventsHistory::new,
        MergeOperators.createBatchMergeOperator(RESOURCE_NAME, PARTITION_NAME, Optional.empty()));
  }

  @Test
  public void testZkSingleEventMerge() throws Exception {

    LeaderEventsHistory leaderEventHistory = new LeaderEventsHistory();

    long now = System.currentTimeMillis();
    LeaderEvent leaderEvent = new LeaderEvent()
        .setOriginating_node("participant1")
        .setEvent_type(LeaderEventType.PARTICIPANT_LEADER_DOWN_INIT)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    LeaderEventsHistory singleEvent = new LeaderEventsHistory();
    singleEvent.addToEvents(leaderEvent);
    zkStore.merge(singleEvent);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    leaderEvent = new LeaderEvent()
        .setOriginating_node("participant1")
        .setEvent_type(LeaderEventType.PARTICIPANT_LEADER_DOWN_SUCCESS)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    singleEvent = new LeaderEventsHistory();
    singleEvent.addToEvents(leaderEvent);
    zkStore.merge(singleEvent);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.SPECTATOR_OBSERVED_LEADER_DOWN)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    singleEvent = new LeaderEventsHistory();
    singleEvent.addToEvents(leaderEvent);
    zkStore.merge(singleEvent);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_DOWN)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    singleEvent = new LeaderEventsHistory();
    singleEvent.addToEvents(leaderEvent);
    zkStore.merge(singleEvent);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    leaderEvent = new LeaderEvent()
        .setOriginating_node("participant2")
        .setEvent_type(LeaderEventType.PARTICIPANT_LEADER_UP_INIT)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    singleEvent = new LeaderEventsHistory();
    singleEvent.addToEvents(leaderEvent);
    zkStore.merge(singleEvent);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_DOWN)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    singleEvent = new LeaderEventsHistory();
    singleEvent.addToEvents(leaderEvent);
    zkStore.merge(singleEvent);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    leaderEvent = new LeaderEvent()
        .setOriginating_node("participant2")
        .setEvent_type(LeaderEventType.PARTICIPANT_LEADER_UP_SUCCESS)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    singleEvent = new LeaderEventsHistory();
    singleEvent.addToEvents(leaderEvent);
    zkStore.merge(singleEvent);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.SPECTATOR_OBSERVED_LEADER_UP)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    singleEvent = new LeaderEventsHistory();
    singleEvent.addToEvents(leaderEvent);
    zkStore.merge(singleEvent);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_UP)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    singleEvent = new LeaderEventsHistory();
    singleEvent.addToEvents(leaderEvent);
    zkStore.merge(singleEvent);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_UP)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    singleEvent = new LeaderEventsHistory();
    singleEvent.addToEvents(leaderEvent);
    zkStore.merge(singleEvent);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());
  }

  @Test
  public void testZkBatchMerge() throws Exception {

    LeaderEventsHistory leaderEventHistory = new LeaderEventsHistory();

    long now = System.currentTimeMillis();

    LeaderEventsHistory batchHistory = new LeaderEventsHistory();

    /** First Batch **/
    LeaderEvent leaderEvent = new LeaderEvent()
        .setOriginating_node("participant1")
        .setEvent_type(LeaderEventType.PARTICIPANT_LEADER_DOWN_INIT)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    batchHistory.addToEvents(leaderEvent);

    leaderEvent = new LeaderEvent()
        .setOriginating_node("participant1")
        .setEvent_type(LeaderEventType.PARTICIPANT_LEADER_DOWN_SUCCESS)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    batchHistory.addToEvents(leaderEvent);

    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    zkStore.merge(batchHistory);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    /** Second Batch **/
    batchHistory.getEvents().clear();

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.SPECTATOR_OBSERVED_LEADER_DOWN)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    batchHistory.addToEvents(leaderEvent);

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_DOWN)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    batchHistory.addToEvents(leaderEvent);

    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    zkStore.merge(batchHistory);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    /** Third Batch **/
    batchHistory.getEvents().clear();

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_DOWN)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    batchHistory.addToEvents(leaderEvent);

    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    zkStore.merge(batchHistory);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    /** Fourth Batch **/
    batchHistory.getEvents().clear();

    leaderEvent = new LeaderEvent()
        .setOriginating_node("participant2")
        .setEvent_type(LeaderEventType.PARTICIPANT_LEADER_UP_INIT)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    batchHistory.addToEvents(leaderEvent);

    leaderEvent = new LeaderEvent()
        .setOriginating_node("participant2")
        .setEvent_type(LeaderEventType.PARTICIPANT_LEADER_UP_SUCCESS)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    batchHistory.addToEvents(leaderEvent);

    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    zkStore.merge(batchHistory);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    /** Fifth Batch **/
    batchHistory.getEvents().clear();

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.SPECTATOR_OBSERVED_LEADER_UP)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    batchHistory.addToEvents(leaderEvent);

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.SPECTATOR_POSTED_SHARDMAP_LEADER_UP)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    batchHistory.addToEvents(leaderEvent);

    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    zkStore.merge(batchHistory);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    /** Sixth Batch **/
    batchHistory.getEvents().clear();

    leaderEvent = new LeaderEvent()
        .setOriginating_node("spectator")
        .setEvent_type(LeaderEventType.CLIENT_OBSERVED_SHARDMAP_LEADER_UP)
        .setEvent_timestamp_ms(++now);

    leaderEventHistory.addToEvents(leaderEvent);
    batchHistory.addToEvents(leaderEvent);

    leaderEventHistory.getEvents().sort(new DescendingTimestampLeaderEventComparator());
    zkStore.merge(batchHistory);
    assertEquals(leaderEventHistory.getEvents(), getData().getEvents());

    final ThriftStringEncoder<LeaderEvent>
        eventJsonEncoder =
        ThriftStringEncoder.createToStringEncoder(LeaderEvent.class);

    for (int eventId = 0; eventId < leaderEventHistory.getEventsSize(); ++eventId) {
      System.out.println(
          String.format("\t event: %s", eventJsonEncoder.encode(leaderEventHistory.getEvents().get(eventId))));
    }

  }

  private LeaderEventsHistory getData() throws Exception {
    return codec.decode(zkClient.getData()
        .forPath(ZkMergeableEventStore
            .getLeaderEventHistoryPath(CLUSTER_NAME, RESOURCE_NAME, PARTITION_NAME)));
  }

  @After
  public void tearDown() throws IOException {
    zkStore.close();
    zkClient.close();
    zkTestServer.stop();
  }
}
