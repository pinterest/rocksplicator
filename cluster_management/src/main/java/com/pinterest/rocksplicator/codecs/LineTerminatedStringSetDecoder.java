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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;

public class LineTerminatedStringSetDecoder implements Decoder<byte[], Set<String>> {

  @Override
  public Set<String> decode(byte[] data) throws CodecException {
    if (data == null) {
      return ImmutableSet.of();
    }

    InputStream inputStream = new ByteArrayInputStream(data);
    InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

    ImmutableSet.Builder<String> setBuilder = ImmutableSet.builder();

    while (true) {
      try {
        String line = null;
        if ((line = bufferedReader.readLine()) == null) {
          break;
        }
        line = line.trim();
        if (line.isEmpty() || line.startsWith("#")) {
          // Comment, move on to next line.
          continue;
        }
        setBuilder.add(line);
      } catch (IOException e) {
        throw new CodecException(e);
      }
    }
    return setBuilder.build();
  }
}
