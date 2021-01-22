package com.pinterest.rocksplicator.codecs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.junit.Test;

public class SnappyCompressionCodecTest extends CodecTestBase {

  @Test
  public void testBinaryCompressionCodec() throws Exception {
    ThriftCodec<LeaderEventsHistory>
        codec =
        ThriftCodec.createBinaryCodec(LeaderEventsHistory.class);
    SnappyCompressionCodec<LeaderEventsHistory> snappyCodec = new SnappyCompressionCodec<>(codec);
    byte[] binaryData = snappyCodec.encode(history);

    LeaderEventsHistory decodedHistory = snappyCodec.decode(binaryData);
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    SnappyCompressionCodec<byte[]>
        binarySnappyCodec =
        new SnappyCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());
    byte[] deserializedData = binarySnappyCodec.decode(binaryData);
    byte[] serializedData = codec.encode(history);
    assertArrayEquals(serializedData, deserializedData);
    assertEquals(history, codec.decode(deserializedData));
  }

  @Test
  public void testCompactCompressionCodec() throws Exception {
    ThriftCodec<LeaderEventsHistory>
        codec =
        ThriftCodec.createCompactCodec(LeaderEventsHistory.class);
    SnappyCompressionCodec<LeaderEventsHistory> snappyCodec = new SnappyCompressionCodec<>(codec);
    byte[] binaryData = snappyCodec.encode(history);
    LeaderEventsHistory decodedHistory = snappyCodec.decode(binaryData);
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    SnappyCompressionCodec<byte[]>
        binarySnappyCodec =
        new SnappyCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());
    byte[] deserializedData = binarySnappyCodec.decode(binaryData);
    byte[] serializedData = codec.encode(history);
    assertArrayEquals(serializedData, deserializedData);
    assertEquals(history, codec.decode(deserializedData));
  }
}
