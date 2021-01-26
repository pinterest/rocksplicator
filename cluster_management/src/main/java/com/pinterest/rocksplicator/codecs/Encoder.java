package com.pinterest.rocksplicator.codecs;

public interface Encoder<S, T> {
  T encode(final S obj) throws CodecException;
}
