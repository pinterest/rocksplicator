package com.pinterest.rocksplicator.codecs;

public interface Codec<S, T> {

  T encode(final S obj) throws CodecException;

  S decode(final T data) throws CodecException;
}
