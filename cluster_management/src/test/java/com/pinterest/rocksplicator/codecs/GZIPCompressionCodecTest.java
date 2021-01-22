package com.pinterest.rocksplicator.codecs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

<<<<<<< HEAD
=======
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
>>>>>>> master
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.junit.Test;

public class GZIPCompressionCodecTest extends CodecTestBase {

  @Test
  public void testBinaryCompressionCodec() throws Exception {
<<<<<<< HEAD
    ThriftCodec<LeaderEventsHistory>
        codec =
        ThriftCodec.createBinaryCodec(LeaderEventsHistory.class);
    GZIPCompressionCodec<LeaderEventsHistory> gzipCodec = new GZIPCompressionCodec<>(codec);
    byte[] binaryData = gzipCodec.encode(history);

    LeaderEventsHistory decodedHistory = gzipCodec.decode(binaryData);
=======
    Codec<LeaderEventsHistory, byte[]> thriftCodec =
        Codecs.createThriftCodec(LeaderEventsHistory.class, SerializationProtocol.BINARY);
    GZIPCompressionCodec<LeaderEventsHistory> gzipCodec = new GZIPCompressionCodec<>(thriftCodec);
    byte[] compressedBinaryData = gzipCodec.encode(history);

    LeaderEventsHistory decodedHistory = gzipCodec.decode(compressedBinaryData);
>>>>>>> master
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    GZIPCompressionCodec<byte[]>
<<<<<<< HEAD
        binaryGZIPCodec =
        new GZIPCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());
    byte[] deserializedData = binaryGZIPCodec.decode(binaryData);
    byte[] serializedData = codec.encode(history);
    assertArrayEquals(serializedData, deserializedData);
    assertEquals(history, codec.decode(deserializedData));
=======
        binaryGZipCodec =
        new GZIPCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());

    byte[] uncompressedData = binaryGZipCodec.decode(compressedBinaryData);
    byte[] serializedData = thriftCodec.encode(history);
    byte[] recompressedData = binaryGZipCodec.encode(serializedData);

    assertArrayEquals(serializedData, uncompressedData);
    assertArrayEquals(recompressedData, compressedBinaryData);
    assertEquals(history, thriftCodec.decode(uncompressedData));
>>>>>>> master
  }

  @Test
  public void testCompactCompressionCodec() throws Exception {
<<<<<<< HEAD
    ThriftCodec<LeaderEventsHistory>
        codec =
        ThriftCodec.createCompactCodec(LeaderEventsHistory.class);
    GZIPCompressionCodec<LeaderEventsHistory> gzipCodec = new GZIPCompressionCodec<>(codec);
    byte[] binaryData = gzipCodec.encode(history);
    LeaderEventsHistory decodedHistory = gzipCodec.decode(binaryData);
=======
    Codec<LeaderEventsHistory, byte[]> thriftCodec =
        Codecs.createThriftCodec(LeaderEventsHistory.class, SerializationProtocol.COMPACT);
    GZIPCompressionCodec<LeaderEventsHistory> gzipCodec = new GZIPCompressionCodec<>(thriftCodec);
    byte[] compressedBinaryData = gzipCodec.encode(history);

    LeaderEventsHistory decodedHistory = gzipCodec.decode(compressedBinaryData);
>>>>>>> master
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    GZIPCompressionCodec<byte[]>
<<<<<<< HEAD
        binaryGZIPCodec =
        new GZIPCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());
    byte[] deserializedData = binaryGZIPCodec.decode(binaryData);
    byte[] serializedData = codec.encode(history);
    assertArrayEquals(serializedData, deserializedData);
    assertEquals(history, codec.decode(deserializedData));
=======
        binaryGZipCodec =
        new GZIPCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());

    byte[] uncompressedData = binaryGZipCodec.decode(compressedBinaryData);
    byte[] serializedData = thriftCodec.encode(history);
    byte[] recompressedData = binaryGZipCodec.encode(serializedData);

    assertArrayEquals(serializedData, uncompressedData);
    assertArrayEquals(recompressedData, compressedBinaryData);
    assertEquals(history, thriftCodec.decode(uncompressedData));
>>>>>>> master
  }
}
