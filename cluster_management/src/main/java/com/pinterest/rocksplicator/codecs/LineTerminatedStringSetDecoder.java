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
