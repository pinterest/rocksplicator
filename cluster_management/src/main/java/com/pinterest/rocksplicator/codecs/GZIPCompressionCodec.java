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
