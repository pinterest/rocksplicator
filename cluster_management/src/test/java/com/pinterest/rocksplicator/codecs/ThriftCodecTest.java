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
