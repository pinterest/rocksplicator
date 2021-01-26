package com.pinterest.rocksplicator.codecs;

public interface Decoder<T, S> {
  S decode(final T data) throws CodecException;
}
