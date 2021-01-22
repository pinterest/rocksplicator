package com.pinterest.rocksplicator.codecs;

public class IdentityCodec<S> implements Codec<S, S> {

  private IdentityCodec() {}

  public static <S> IdentityCodec<S> createIdentityCodec() {
    return new IdentityCodec<S>();
  }

  @Override
  public S encode(S obj) throws CodecException {
    return obj;
  }

  @Override
  public S decode(S data) throws CodecException {
    return data;
  }
}
