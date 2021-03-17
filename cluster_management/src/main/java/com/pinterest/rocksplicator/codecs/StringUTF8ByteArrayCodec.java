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

import java.io.UnsupportedEncodingException;

/**
 * Codec to encode String into byte array with utf-8 encoding and
 * decoding a given byte array
 * with utf-8 encoding into java string object.
 */
public class StringUTF8ByteArrayCodec implements Codec<String, byte[]> {

  @Override
  public String decode(byte[] data) throws CodecException {
    try {
      return new String(data, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new CodecException(e);
    }
  }

  @Override
  public byte[] encode(String obj) throws CodecException {
    try {
      return obj.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new CodecException(e);
    }
  }
}
