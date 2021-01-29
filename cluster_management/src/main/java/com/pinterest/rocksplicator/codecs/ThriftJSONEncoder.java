package com.pinterest.rocksplicator.codecs;

import com.google.common.base.Preconditions;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayOutputStream;

public class ThriftJSONEncoder<S extends TBase<S, ?>> implements Encoder<S, String> {

  private final TProtocolFactory protocolFactory;
  private final Class<S> thriftClazz;

  ThriftJSONEncoder(Class<S> thriftClazz, TProtocolFactory protocolFactory) {
    this.protocolFactory = protocolFactory;
    this.thriftClazz = Preconditions.checkNotNull(thriftClazz);
  }

  public static <S extends TBase<S, ?>> ThriftJSONEncoder<S> createJSONEncoder(
      Class<S> thriftClazz) {
    return new ThriftJSONEncoder<>(thriftClazz, new TSimpleJSONProtocol.Factory());
  }

  public static <S extends TBase<S, ?>> ThriftJSONEncoder<S> createSimpleJSONEncoder(
      Class<S> thriftClazz) {
    return new ThriftJSONEncoder<>(thriftClazz, new TSimpleJSONProtocol.Factory());
  }

  @Override
  public String encode(S obj) throws CodecException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    TIOStreamTransport transport = new TIOStreamTransport(bos);
    TProtocol protocol = protocolFactory.getProtocol(transport);
    try {
      obj.write(protocol);
      protocol.getTransport().flush();
      protocol.getTransport().close();
    } catch (
        TException e) {
      throw new CodecException("Encoding Error:", e);
    }
    return new String(bos.toByteArray());
  }
}
