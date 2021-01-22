package com.pinterest.rocksplicator.codecs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.junit.Test;

public class GZIPCompressionCodecTest extends CodecTestBase {

  @Test
  public void testBinaryCompressionCodec() throws Exception {
    Codec<LeaderEventsHistory, byte[]> thriftCodec =
        Codecs.createThriftCodec(LeaderEventsHistory.class, SerializationProtocol.BINARY);
    GZIPCompressionCodec<LeaderEventsHistory> gzipCodec = new GZIPCompressionCodec<>(thriftCodec);
    byte[] compressedBinaryData = gzipCodec.encode(history);

    LeaderEventsHistory decodedHistory = gzipCodec.decode(compressedBinaryData);
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    GZIPCompressionCodec<byte[]>
        binaryGZipCodec =
        new GZIPCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());

    byte[] uncompressedData = binaryGZipCodec.decode(compressedBinaryData);
    byte[] serializedData = thriftCodec.encode(history);
    byte[] recompressedData = binaryGZipCodec.encode(serializedData);

    assertArrayEquals(serializedData, uncompressedData);
    assertArrayEquals(recompressedData, compressedBinaryData);
    assertEquals(history, thriftCodec.decode(uncompressedData));
  }

  @Test
  public void testCompactCompressionCodec() throws Exception {
    Codec<LeaderEventsHistory, byte[]> thriftCodec =
        Codecs.createThriftCodec(LeaderEventsHistory.class, SerializationProtocol.COMPACT);
    GZIPCompressionCodec<LeaderEventsHistory> gzipCodec = new GZIPCompressionCodec<>(thriftCodec);
    byte[] compressedBinaryData = gzipCodec.encode(history);

    LeaderEventsHistory decodedHistory = gzipCodec.decode(compressedBinaryData);
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    GZIPCompressionCodec<byte[]>
        binaryGZipCodec =
        new GZIPCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());

    byte[] uncompressedData = binaryGZipCodec.decode(compressedBinaryData);
    byte[] serializedData = thriftCodec.encode(history);
    byte[] recompressedData = binaryGZipCodec.encode(serializedData);

    assertArrayEquals(serializedData, uncompressedData);
    assertArrayEquals(recompressedData, compressedBinaryData);
    assertEquals(history, thriftCodec.decode(uncompressedData));
  }
}
