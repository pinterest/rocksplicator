package com.pinterest.rocksplicator.codecs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.eventhistory.LeaderEventsHistory;

import org.junit.Test;

public class WrappedDataThriftCodecTest extends CodecTestBase {

  @Test
  public void testWrappedData() throws CodecException {
    WrappedDataThriftCodec<LeaderEventsHistory>
        wrappedCodecBU =
        new WrappedDataThriftCodec(LeaderEventsHistory.class, SerializationProtocol.BINARY,
            CompressionAlgorithm.UNCOMPRESSED);
    WrappedDataThriftCodec<LeaderEventsHistory>
        wrappedCodecBG =
        new WrappedDataThriftCodec(LeaderEventsHistory.class, SerializationProtocol.BINARY,
            CompressionAlgorithm.GZIP);
    WrappedDataThriftCodec<LeaderEventsHistory>
        wrappedCodecBS =
        new WrappedDataThriftCodec(LeaderEventsHistory.class, SerializationProtocol.BINARY,
            CompressionAlgorithm.SNAPPY);
    WrappedDataThriftCodec<LeaderEventsHistory>
        wrappedCodecCU =
        new WrappedDataThriftCodec(LeaderEventsHistory.class, SerializationProtocol.COMPACT,
            CompressionAlgorithm.UNCOMPRESSED);
    WrappedDataThriftCodec<LeaderEventsHistory>
        wrappedCodecCG =
        new WrappedDataThriftCodec(LeaderEventsHistory.class, SerializationProtocol.COMPACT,
            CompressionAlgorithm.GZIP);
    WrappedDataThriftCodec<LeaderEventsHistory>
        wrappedCodecCS =
        new WrappedDataThriftCodec(LeaderEventsHistory.class, SerializationProtocol.COMPACT,
            CompressionAlgorithm.SNAPPY);

    byte[] buBytes = wrappedCodecBU.encode(history);
    byte[] bgBytes = wrappedCodecBG.encode(history);
    byte[] bsBytes = wrappedCodecBS.encode(history);
    byte[] cuBytes = wrappedCodecCU.encode(history);
    byte[] cgBytes = wrappedCodecCG.encode(history);
    byte[] csBytes = wrappedCodecCS.encode(history);

    assertTrue(buBytes.length > bgBytes.length);
    assertTrue(buBytes.length > bsBytes.length);
    assertTrue(cuBytes.length > cgBytes.length);
    assertTrue(cuBytes.length > csBytes.length);

    // Ensure all of the above serialized versions can be deserialized any of the above codecs.
    assertEquals(history, wrappedCodecBU.decode(buBytes));
    assertEquals(history, wrappedCodecBU.decode(bgBytes));
    assertEquals(history, wrappedCodecBU.decode(bsBytes));
    assertEquals(history, wrappedCodecBU.decode(cuBytes));
    assertEquals(history, wrappedCodecBU.decode(cgBytes));
    assertEquals(history, wrappedCodecBU.decode(csBytes));

    assertEquals(history, wrappedCodecBG.decode(buBytes));
    assertEquals(history, wrappedCodecBG.decode(bgBytes));
    assertEquals(history, wrappedCodecBG.decode(bsBytes));
    assertEquals(history, wrappedCodecBG.decode(cuBytes));
    assertEquals(history, wrappedCodecBG.decode(cgBytes));
    assertEquals(history, wrappedCodecBG.decode(csBytes));

    assertEquals(history, wrappedCodecBS.decode(buBytes));
    assertEquals(history, wrappedCodecBS.decode(bgBytes));
    assertEquals(history, wrappedCodecBS.decode(bsBytes));
    assertEquals(history, wrappedCodecBS.decode(cuBytes));
    assertEquals(history, wrappedCodecBS.decode(cgBytes));
    assertEquals(history, wrappedCodecBS.decode(csBytes));

    assertEquals(history, wrappedCodecCU.decode(buBytes));
    assertEquals(history, wrappedCodecCU.decode(bgBytes));
    assertEquals(history, wrappedCodecCU.decode(bsBytes));
    assertEquals(history, wrappedCodecCU.decode(cuBytes));
    assertEquals(history, wrappedCodecCU.decode(cgBytes));
    assertEquals(history, wrappedCodecCU.decode(csBytes));

    assertEquals(history, wrappedCodecCG.decode(buBytes));
    assertEquals(history, wrappedCodecCG.decode(bgBytes));
    assertEquals(history, wrappedCodecCG.decode(bsBytes));
    assertEquals(history, wrappedCodecCG.decode(cuBytes));
    assertEquals(history, wrappedCodecCG.decode(cgBytes));
    assertEquals(history, wrappedCodecCG.decode(csBytes));

    assertEquals(history, wrappedCodecCS.decode(buBytes));
    assertEquals(history, wrappedCodecCS.decode(bgBytes));
    assertEquals(history, wrappedCodecCS.decode(bsBytes));
    assertEquals(history, wrappedCodecCS.decode(cuBytes));
    assertEquals(history, wrappedCodecCS.decode(cgBytes));
    assertEquals(history, wrappedCodecCS.decode(csBytes));
  }
}
