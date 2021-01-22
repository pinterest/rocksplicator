package com.pinterest.rocksplicator.codecs;

import com.google.common.base.Preconditions;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class SnappyCompressionCodec<S> implements BinaryArrayCodec<S> {
  private final Codec<S, byte[]> delegate;

  public SnappyCompressionCodec(Codec<S, byte[]> delegate) {
    this.delegate = Preconditions.checkNotNull(delegate);
  }

  @Override
  public byte[] encode(S obj) throws CodecException {
    try {
      byte[] uncompressedData = delegate.encode(obj);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      SnappyOutputStream sos = new SnappyOutputStream(bos);
      sos.write(uncompressedData);
      sos.flush(); sos.close();
      return bos.toByteArray();
    } catch (IOException e) {
      throw new CodecException(e);
    }
  }

  @Override
  public S decode(byte[] data) throws CodecException {
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(data);
      SnappyInputStream sis = new SnappyInputStream(bis);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      byte[] buffer = new byte[128];
      while (true ) {
        int read_bytes = sis.read(buffer);
        if (read_bytes != 0) {
          bos.write(buffer, 0, read_bytes);
        }
        if (read_bytes < 128) {
          break;
        }
      }
      return delegate.decode(bos.toByteArray());
    } catch (IOException e) {
      throw new CodecException(e);
    }
  }
}
