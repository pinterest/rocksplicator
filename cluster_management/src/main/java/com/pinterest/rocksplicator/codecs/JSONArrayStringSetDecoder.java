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

import com.google.common.collect.ImmutableSet;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.UnsupportedEncodingException;
import java.util.Set;

/**
 * Decodes byte[] content as a JSONArray
 * ["first", "second", "third"]
 */
public class JSONArrayStringSetDecoder implements Decoder<byte[], Set<String>> {

  @Override
  public Set<String> decode(byte[] data) throws CodecException {
    JSONParser parser = new JSONParser();
    try {
      JSONArray array = (JSONArray) parser.parse(new String(data, "UTF-8"));
      return ImmutableSet.copyOf(array);
    } catch (ParseException e) {
      throw new CodecException(e);
    } catch (UnsupportedEncodingException e) {
      throw new CodecException(e);
    }
  }
}
