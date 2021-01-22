package com.pinterest.rocksplicator.codecs;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class SnappyCompressionCodec<S> extends AbstractCompressionCodec<S> {

  SnappyCompressionCodec(Codec<S, byte[]> delegate) {
    super(delegate);
  }

  @Override
  protected OutputStream createCompressedOutputStream(OutputStream stream) throws IOException {
    return new SnappyOutputStream(stream);
  }

  @Override
  protected InputStream createDecompressedInputStream(InputStream stream) throws IOException {
    return new SnappyInputStream(stream);
  }
}
