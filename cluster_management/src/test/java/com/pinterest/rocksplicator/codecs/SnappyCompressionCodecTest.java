package com.pinterest.rocksplicator.codecs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

<<<<<<< HEAD
=======
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
>>>>>>> master
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.junit.Test;

public class SnappyCompressionCodecTest extends CodecTestBase {

  @Test
  public void testBinaryCompressionCodec() throws Exception {
<<<<<<< HEAD
    ThriftCodec<LeaderEventsHistory>
        codec =
        ThriftCodec.createBinaryCodec(LeaderEventsHistory.class);
    SnappyCompressionCodec<LeaderEventsHistory> snappyCodec = new SnappyCompressionCodec<>(codec);
    byte[] binaryData = snappyCodec.encode(history);

    LeaderEventsHistory decodedHistory = snappyCodec.decode(binaryData);
=======
    Codec<LeaderEventsHistory, byte[]> thriftCodec =
        Codecs.createThriftCodec(LeaderEventsHistory.class, SerializationProtocol.BINARY);
    SnappyCompressionCodec<LeaderEventsHistory> snappyCodec = new SnappyCompressionCodec<>(thriftCodec);
    byte[] compressedBinaryData = snappyCodec.encode(history);

    LeaderEventsHistory decodedHistory = snappyCodec.decode(compressedBinaryData);
>>>>>>> master
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    SnappyCompressionCodec<byte[]>
        binarySnappyCodec =
        new SnappyCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());
<<<<<<< HEAD
    byte[] deserializedData = binarySnappyCodec.decode(binaryData);
    byte[] serializedData = codec.encode(history);
    assertArrayEquals(serializedData, deserializedData);
    assertEquals(history, codec.decode(deserializedData));
=======

    byte[] uncompressedData = binarySnappyCodec.decode(compressedBinaryData);
    byte[] serializedData = thriftCodec.encode(history);
    byte[] recompressedData = binarySnappyCodec.encode(serializedData);

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
    SnappyCompressionCodec<LeaderEventsHistory> snappyCodec = new SnappyCompressionCodec<>(codec);
    byte[] binaryData = snappyCodec.encode(history);
    LeaderEventsHistory decodedHistory = snappyCodec.decode(binaryData);
=======
    Codec<LeaderEventsHistory, byte[]> thriftCodec =
        Codecs.createThriftCodec(LeaderEventsHistory.class, SerializationProtocol.COMPACT);
    SnappyCompressionCodec<LeaderEventsHistory> snappyCodec = new SnappyCompressionCodec<>(thriftCodec);
    byte[] compressedBinaryData = snappyCodec.encode(history);

    LeaderEventsHistory decodedHistory = snappyCodec.decode(compressedBinaryData);
>>>>>>> master
    assertEquals(history, decodedHistory);

    // Ensure binaryData is decodable seperately.
    SnappyCompressionCodec<byte[]>
        binarySnappyCodec =
        new SnappyCompressionCodec<>(IdentityCodec.<byte[]>createIdentityCodec());
<<<<<<< HEAD
    byte[] deserializedData = binarySnappyCodec.decode(binaryData);
    byte[] serializedData = codec.encode(history);
    assertArrayEquals(serializedData, deserializedData);
    assertEquals(history, codec.decode(deserializedData));
=======

    byte[] uncompressedData = binarySnappyCodec.decode(compressedBinaryData);
    byte[] serializedData = thriftCodec.encode(history);
    byte[] recompressedData = binarySnappyCodec.encode(serializedData);

    assertArrayEquals(serializedData, uncompressedData);
    assertArrayEquals(recompressedData, compressedBinaryData);
    assertEquals(history, thriftCodec.decode(uncompressedData));
>>>>>>> master
  }
}
