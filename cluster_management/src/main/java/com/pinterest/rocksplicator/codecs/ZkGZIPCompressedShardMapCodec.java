package com.pinterest.rocksplicator.codecs;

import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;

import org.json.simple.JSONObject;

public class ZkGZIPCompressedShardMapCodec implements Codec<JSONObject, byte[]> {

  private static final Codec<JSONObject, byte[]> baseCodec = new JSONObjectCodec();
  private static final Codec<JSONObject, byte[]> gzipCompressedCoded =
      Codecs.getCompressedCodec(baseCodec, CompressionAlgorithm.GZIP);


  @Override
  public JSONObject decode(byte[] data) throws CodecException {
    return gzipCompressedCoded.decode(data);
  }

  @Override
  public byte[] encode(JSONObject obj) throws CodecException {
    return gzipCompressedCoded.encode(obj);
  }
}
