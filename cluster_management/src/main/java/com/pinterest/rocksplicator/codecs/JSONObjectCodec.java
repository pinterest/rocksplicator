package com.pinterest.rocksplicator.codecs;

import org.json.simple.JSONObject;

import java.io.UnsupportedEncodingException;

public class JSONObjectCodec implements Codec<JSONObject, byte[]> {

  @Override
  public byte[] encode(JSONObject obj) throws CodecException {
    try {
      return obj.toJSONString().getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new CodecException(e);
    }
  }

  @Override
  public JSONObject decode(byte[] data) throws CodecException {
    return new SimpleJsonObjectDecoder().decode(data);
  }
}
