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

package com.pinterest.rocksplicator.config;

import com.pinterest.rocksplicator.codecs.Decoder;
import com.pinterest.rocksplicator.codecs.JSONArrayStringSetDecoder;
import com.pinterest.rocksplicator.codecs.LineTerminatedStringSetDecoder;

import java.io.UnsupportedEncodingException;
import java.util.Set;

public class ConfigCodecs {

  public static Decoder<byte[], Set<String>> getDecoder(String decoder_enum)
      throws UnsupportedEncodingException {
    if ("line_terminated".equalsIgnoreCase(decoder_enum)) {
      return getDecoder(ConfigCodecEnum.LINE_TERMINATED);
    } else if ("json_array".equalsIgnoreCase(decoder_enum)) {
      return getDecoder(ConfigCodecEnum.JSON_ARRAY);
    } else {
      throw new UnsupportedEncodingException("Encoding not supported");
    }
  }

  public static Decoder<byte[], Set<String>> getDecoder(ConfigCodecEnum cce)
      throws UnsupportedEncodingException {
    switch (cce) {
      case LINE_TERMINATED:
        return new LineTerminatedStringSetDecoder();
      case JSON_ARRAY:
        return new JSONArrayStringSetDecoder();
      default:
        throw new UnsupportedEncodingException("Encoding not supported");
    }
  }
}
