package com.pinterest.rocksplicator.codecs;

import org.json.simple.JSONObject;

public class JSONObjectCodec implements Codec<JSONObject, byte[]> {

  @Override
  public byte[] encode(JSONObject obj) throws CodecException {
    return obj.toJSONString().getBytes();
  }

  @Override
  public JSONObject decode(byte[] data) throws CodecException {
    return new SimpleJsonObjectDecoder().decode(data);
  }
}
