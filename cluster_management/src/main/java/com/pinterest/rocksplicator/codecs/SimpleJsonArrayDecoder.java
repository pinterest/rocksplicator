package com.pinterest.rocksplicator.codecs;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.UnsupportedEncodingException;

public class SimpleJsonArrayDecoder implements Decoder<byte[], JSONArray> {

  @Override
  public JSONArray decode(byte[] data) throws CodecException {
    JSONParser parser = new JSONParser();
    try {
      return (JSONArray) parser.parse(new String(data, "UTF-8"));
    } catch (ParseException e) {
      throw new CodecException(e);
    } catch (UnsupportedEncodingException e) {
      throw new CodecException(e);
    }
  }
}
