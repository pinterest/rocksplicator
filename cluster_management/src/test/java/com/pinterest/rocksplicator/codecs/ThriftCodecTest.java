package com.pinterest.rocksplicator.codecs;

import static org.junit.Assert.assertEquals;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.junit.Test;

public class ThriftCodecTest extends CodecTestBase {

  @Test
  public void testBinaryCodec() throws Exception {
    ThriftCodec<LeaderEventsHistory>
        codec =
        ThriftCodec.createBinaryCodec(LeaderEventsHistory.class);
    byte[] binaryData = codec.encode(history);
    LeaderEventsHistory decodedHistory = codec.decode(binaryData);
    assertEquals(history, decodedHistory);
  }

  @Test
  public void testCompactCodec() throws Exception {
    ThriftCodec<LeaderEventsHistory>
        codec =
        ThriftCodec.createCompactCodec(LeaderEventsHistory.class);
    byte[] binaryData = codec.encode(history);
    LeaderEventsHistory decodedHistory = codec.decode(binaryData);
    assertEquals(history, decodedHistory);
  }
}
