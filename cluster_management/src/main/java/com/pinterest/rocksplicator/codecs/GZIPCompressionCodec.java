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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

class GZIPCompressionCodec<S> extends AbstractCompressionCodec<S> {

  GZIPCompressionCodec(Codec<S, byte[]> delegate) {
    super(delegate);
  }

  @Override
  protected OutputStream createCompressedOutputStream(OutputStream stream) throws IOException {
    return new GZIPOutputStream(stream);
  }

  @Override
  protected InputStream createDecompressedInputStream(InputStream stream) throws IOException {
    return new GZIPInputStream(stream);
  }
}
