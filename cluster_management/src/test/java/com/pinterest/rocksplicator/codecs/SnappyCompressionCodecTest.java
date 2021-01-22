package com.pinterest.rocksplicator.codecs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.junit.Test;

public class SnappyCompressionCodecTest extends CodecTestBase {

  @Test
  public void testBinaryCompressionCodec() throws Exception {
    Codec<LeaderEventsHistory, byte[]> thriftCodec =
        Codecs.createThriftCodec(LeaderEventsHistory.class, SerializationProtocol.BINARY);
    SnappyCompressionCodec<LeaderEventsHistory> snappyCodec = new SnappyCompressionCodec<>(thriftCodec);
    byte[] binaryData = snappyCodec.encode(history);

    LeaderEventsHistory decodedHistory = snappyCodec.decode(binaryData);
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    SnappyCompressionCodec<byte[]>
        binarySnappyCodec =
        new SnappyCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());
    byte[] deserializedData = binarySnappyCodec.decode(binaryData);
    byte[] serializedData = thriftCodec.encode(history);
    assertArrayEquals(serializedData, deserializedData);
    assertEquals(history, thriftCodec.decode(deserializedData));
  }

  @Test
  public void testCompactCompressionCodec() throws Exception {
    Codec<LeaderEventsHistory, byte[]> thriftCodec =
        Codecs.createThriftCodec(LeaderEventsHistory.class, SerializationProtocol.COMPACT);
    SnappyCompressionCodec<LeaderEventsHistory> snappyCodec = new SnappyCompressionCodec<>(thriftCodec);
    byte[] binaryData = snappyCodec.encode(history);
    LeaderEventsHistory decodedHistory = snappyCodec.decode(binaryData);
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    SnappyCompressionCodec<byte[]>
        binarySnappyCodec =
        new SnappyCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());
    byte[] deserializedData = binarySnappyCodec.decode(binaryData);
    byte[] serializedData = thriftCodec.encode(history);
    assertArrayEquals(serializedData, deserializedData);
    assertEquals(history, thriftCodec.decode(deserializedData));
  }
}
