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
