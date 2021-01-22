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
