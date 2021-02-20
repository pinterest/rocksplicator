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

import org.json.simple.JSONObject;

/**
 * Codec for serializing a simple JSONObject into byte array
 * and deserializing a byte array into simple JSONObject
 */
public class SimpleJsonObjectByteArrayCodec implements Codec<JSONObject, byte[]> {

  private static final StringUTF8ByteArrayCodec
      STRING_UTF_8_BYTE_ARRAY_CODEC = new StringUTF8ByteArrayCodec();
  private static final SimpleJsonObjectStringCodec
      SIMPLE_JSON_OBJECT_STRING_CODEC = new SimpleJsonObjectStringCodec();

  @Override
  public JSONObject decode(byte[] data) throws CodecException {
    return SIMPLE_JSON_OBJECT_STRING_CODEC.decode(STRING_UTF_8_BYTE_ARRAY_CODEC.decode(data));
  }


  @Override
  public byte[] encode(JSONObject obj) throws CodecException {
    return STRING_UTF_8_BYTE_ARRAY_CODEC.encode(SIMPLE_JSON_OBJECT_STRING_CODEC.encode(obj));
  }
}
