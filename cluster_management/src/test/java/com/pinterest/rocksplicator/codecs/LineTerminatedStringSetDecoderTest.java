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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.Set;

public class LineTerminatedStringSetDecoderTest {

  @Test
  public void testSimpleConfig() throws Exception {
    String config = new StringBuilder()
        .append("#This is comment\n")
        .append("first_line\n")
        .append("\n") // Empty line
        .append("   \n") // Empty line with white space characters
        .append("second line as a whole\n")
        .append("   third_line\n") // third line with leading white space characters
        .append("fourth_line   \n") // fourth line with trailing white space characters
        .toString();

    LineTerminatedStringSetDecoder decoder = new LineTerminatedStringSetDecoder();

    Set<String> stringSet = decoder.decode(config.getBytes());

    System.out.println(stringSet);

    assertTrue(stringSet.contains("first_line"));
    assertTrue(stringSet.contains("second line as a whole"));
    assertTrue(stringSet.contains("third_line"));
    assertTrue(stringSet.contains("fourth_line"));
    assertFalse(stringSet.contains(""));
    assertFalse(stringSet.contains("#This is comment"));
    assertTrue(stringSet.size() == 4);
  }
}
