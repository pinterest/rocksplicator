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
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.UnsupportedEncodingException;

public class SimpleJsonObjectDecoder implements Decoder<byte[], JSONObject> {

  @Override
  public JSONObject decode(byte[] data) throws CodecException {
    JSONParser parser = new JSONParser();
    try {
      return (JSONObject) parser.parse(new String(data, "UTF-8"));
    } catch (ParseException e) {
      throw new CodecException(e);
    } catch (UnsupportedEncodingException e) {
      throw new CodecException(e);
    }
  }
}
