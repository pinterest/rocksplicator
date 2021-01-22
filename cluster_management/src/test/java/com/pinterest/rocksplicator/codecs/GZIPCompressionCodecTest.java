package com.pinterest.rocksplicator.codecs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.junit.Test;

public class GZIPCompressionCodecTest extends CodecTestBase {

  @Test
  public void testBinaryCompressionCodec() throws Exception {
    ThriftCodec<LeaderEventsHistory>
        codec =
        ThriftCodec.createBinaryCodec(LeaderEventsHistory.class);
    GZIPCompressionCodec<LeaderEventsHistory> gzipCodec = new GZIPCompressionCodec<>(codec);
    byte[] binaryData = gzipCodec.encode(history);

    LeaderEventsHistory decodedHistory = gzipCodec.decode(binaryData);
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    GZIPCompressionCodec<byte[]>
        binaryGZIPCodec =
        new GZIPCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());
    byte[] deserializedData = binaryGZIPCodec.decode(binaryData);
    byte[] serializedData = codec.encode(history);
    assertArrayEquals(serializedData, deserializedData);
    assertEquals(history, codec.decode(deserializedData));
  }

  @Test
  public void testCompactCompressionCodec() throws Exception {
    ThriftCodec<LeaderEventsHistory>
        codec =
        ThriftCodec.createCompactCodec(LeaderEventsHistory.class);
    GZIPCompressionCodec<LeaderEventsHistory> gzipCodec = new GZIPCompressionCodec<>(codec);
    byte[] binaryData = gzipCodec.encode(history);
    LeaderEventsHistory decodedHistory = gzipCodec.decode(binaryData);
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    GZIPCompressionCodec<byte[]>
        binaryGZIPCodec =
        new GZIPCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());
    byte[] deserializedData = binaryGZIPCodec.decode(binaryData);
    byte[] serializedData = codec.encode(history);
    assertArrayEquals(serializedData, deserializedData);
    assertEquals(history, codec.decode(deserializedData));
  }
}
