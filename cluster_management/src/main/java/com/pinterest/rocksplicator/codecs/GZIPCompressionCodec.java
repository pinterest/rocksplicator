package com.pinterest.rocksplicator.codecs;

import com.google.common.base.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GZIPCompressionCodec<S> implements BinaryArrayCodec<S> {
  private final Codec<S, byte[]> delegate;

  public GZIPCompressionCodec(Codec<S, byte[]> delegate) {
    this.delegate = Preconditions.checkNotNull(delegate);
  }

  @Override
  public byte[] encode(S obj) throws CodecException {
    try {
      byte[] uncompressedData = delegate.encode(obj);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      GZIPOutputStream gos = new GZIPOutputStream(bos);
      gos.write(uncompressedData);
      gos.flush(); gos.close();
      return bos.toByteArray();
    } catch (IOException e) {
      throw new CodecException(e);
    }
  }

  @Override
  public S decode(byte[] data) throws CodecException {
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(data);
      GZIPInputStream gis = new GZIPInputStream(bis);
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      byte[] buffer = new byte[128];
      while (true ) {
        int read_bytes = gis.read(buffer);
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
