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

import com.google.common.base.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class AbstractCompressionCodec<S> implements BinaryArrayCodec<S> {

  private final Codec<S, byte[]> delegate;

  public AbstractCompressionCodec(Codec<S, byte[]> delegate) {
    this.delegate = Preconditions.checkNotNull(delegate);
  }

  protected abstract OutputStream createCompressedOutputStream(OutputStream stream)
      throws IOException;

  protected abstract InputStream createDecompressedInputStream(InputStream stream)
      throws IOException;

  @Override
  public byte[] encode(S obj) throws CodecException {
    try {
      byte[] uncompressedData = delegate.encode(obj);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      OutputStream compressedOutputStream = createCompressedOutputStream(bos);
      compressedOutputStream.write(uncompressedData);
      compressedOutputStream.flush();
      compressedOutputStream.close();
      return bos.toByteArray();
    } catch (IOException e) {
      throw new CodecException(e);
    }
  }

  @Override
  public S decode(byte[] data) throws CodecException {
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(data);
      InputStream decompressedInputStream = createDecompressedInputStream(bis);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      byte[] buffer = new byte[128];
      while (true) {
        int read_bytes = decompressedInputStream.read(buffer);
        if (read_bytes < 0) {
          break;
        }
        bos.write(buffer, 0, read_bytes);
      }
      decompressedInputStream.close();
      return delegate.decode(bos.toByteArray());
    } catch (IOException e) {
      throw new CodecException(e);
    }
  }
}
