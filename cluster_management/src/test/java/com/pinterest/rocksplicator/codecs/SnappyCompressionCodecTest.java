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
    byte[] compressedBinaryData = snappyCodec.encode(history);

    LeaderEventsHistory decodedHistory = snappyCodec.decode(compressedBinaryData);
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    SnappyCompressionCodec<byte[]>
        binarySnappyCodec =
        new SnappyCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());

    byte[] uncompressedData = binarySnappyCodec.decode(compressedBinaryData);
    byte[] serializedData = thriftCodec.encode(history);
    byte[] recompressedData = binarySnappyCodec.encode(serializedData);

    assertArrayEquals(serializedData, uncompressedData);
    assertArrayEquals(recompressedData, compressedBinaryData);
    assertEquals(history, thriftCodec.decode(uncompressedData));
  }

  @Test
  public void testCompactCompressionCodec() throws Exception {
    Codec<LeaderEventsHistory, byte[]> thriftCodec =
        Codecs.createThriftCodec(LeaderEventsHistory.class, SerializationProtocol.COMPACT);
    SnappyCompressionCodec<LeaderEventsHistory> snappyCodec = new SnappyCompressionCodec<>(thriftCodec);
    byte[] compressedBinaryData = snappyCodec.encode(history);

    LeaderEventsHistory decodedHistory = snappyCodec.decode(compressedBinaryData);
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    SnappyCompressionCodec<byte[]>
        binarySnappyCodec =
        new SnappyCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());

    byte[] uncompressedData = binarySnappyCodec.decode(compressedBinaryData);
    byte[] serializedData = thriftCodec.encode(history);
    byte[] recompressedData = binarySnappyCodec.encode(serializedData);

    assertArrayEquals(serializedData, uncompressedData);
    assertArrayEquals(recompressedData, compressedBinaryData);
    assertEquals(history, thriftCodec.decode(uncompressedData));
  }
}
