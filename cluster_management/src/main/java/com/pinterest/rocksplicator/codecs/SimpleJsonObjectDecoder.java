package com.pinterest.rocksplicator.codecs;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.UnsupportedEncodingException;

public class SimpleJsonObjectDecoder implements Decoder<byte[], JSONObject> {

  @Override
  public JSONObject decode(byte[] data) throws CodecException {
    JSONParser parser = new JSONParser();
    try {
      return (JSONObject) parser.parse(new String(data, "UTF-8"));
    } catch (ParseException e) {
      throw new CodecException(e);
    } catch (UnsupportedEncodingException e) {
      throw new CodecException(e);
    }
  }
}
