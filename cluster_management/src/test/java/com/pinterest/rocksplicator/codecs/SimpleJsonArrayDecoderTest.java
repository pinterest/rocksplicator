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
import static org.junit.Assert.assertTrue;

import org.json.simple.JSONArray;
import org.junit.Test;

public class SimpleJsonArrayDecoderTest {

  @Test
  public void testSimpleConfig() throws Exception {
    String config = new StringBuilder()
        .append("[")
        .append("\"first\"")
        .append(",")
        .append("\"second\"")
        .append("\"second\"")
        .append(",")
        .append("\"third\"")
        .append(",")
        .append("\"fourth\"")
        .append(",")
        .append("]")
        .toString();

    SimpleJsonArrayDecoder decoder = new SimpleJsonArrayDecoder();

    JSONArray array = decoder.decode(config.getBytes());

    System.out.println(String.format("json string:->%s, json array object:->%s", config, array));

    assertEquals("first", array.get(0));
    assertEquals("second", array.get(1));
    assertEquals("second", array.get(2));
    assertEquals("third", array.get(3));
    assertEquals("fourth", array.get(4));
    assertTrue(array.size() == 5);
  }

  @Test
  public void testSimpleJSONArrayConfig() throws Exception {
    JSONArray srcArray = new JSONArray();
    srcArray.add("first");
    srcArray.add("second");
    srcArray.add("third");
    srcArray.add("fourth");
    srcArray.add("fourth");

    SimpleJsonArrayDecoder decoder = new SimpleJsonArrayDecoder();

    JSONArray decodecArray = decoder.decode(srcArray.toJSONString().getBytes());

    System.out.println(String
        .format("json array obj (src):->%s, json array obj (dest):->%s", srcArray.toJSONString(),
            decodecArray));

    assertEquals("first", decodecArray.get(0));
    assertEquals("second", decodecArray.get(1));
    assertEquals("third", decodecArray.get(2));
    assertEquals("fourth", decodecArray.get(3));
    assertEquals("fourth", decodecArray.get(4));
    assertTrue(decodecArray.size() == 5);
  }
}
