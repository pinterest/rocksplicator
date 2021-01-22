package com.pinterest.rocksplicator.codecs;

import static org.junit.Assert.assertEquals;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Test;

public class ThriftCodecTest extends CodecTestBase {

  @Test
  public void testBinaryCodec() throws Exception {
    ThriftCodec<LeaderEventsHistory>
        codec = new ThriftCodec<>(LeaderEventsHistory.class, new TBinaryProtocol.Factory());
    byte[] binaryData = codec.encode(history);
    LeaderEventsHistory decodedHistory = codec.decode(binaryData);
    assertEquals(history, decodedHistory);
  }

  @Test
  public void testCompactCodec() throws Exception {
    ThriftCodec<LeaderEventsHistory>
        codec = new ThriftCodec<>(LeaderEventsHistory.class, new TCompactProtocol.Factory());
    byte[] binaryData = codec.encode(history);
    LeaderEventsHistory decodedHistory = codec.decode(binaryData);
    assertEquals(history, decodedHistory);
  }
}
