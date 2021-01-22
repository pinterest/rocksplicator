package com.pinterest.rocksplicator.codecs;

public interface Codec<S, T> {
<<<<<<< HEAD
  T encode(final S obj) throws CodecException;
=======

  T encode(final S obj) throws CodecException;

>>>>>>> master
  S decode(final T data) throws CodecException;
}
