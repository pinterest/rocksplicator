package com.pinterest.rocksplicator.codecs;

import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;

import com.google.common.base.Preconditions;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

class ThriftCodec<S extends TBase<S, ?>> implements BinaryArrayCodec<S> {

  private final TProtocolFactory protocolFactory;
  private final Class<S> clazz;

  ThriftCodec(Class<S> clazz, TProtocolFactory protocolFactory) {
    Preconditions.checkNotNull(protocolFactory);
    this.protocolFactory = protocolFactory;
    this.clazz = Preconditions.checkNotNull(clazz);
  }

  @Override
  public byte[] encode(S obj) throws CodecException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    TIOStreamTransport transport = new TIOStreamTransport(bos);
    TProtocol protocol = protocolFactory.getProtocol(transport);
    try {
      obj.write(protocol);
      protocol.getTransport().flush();
      protocol.getTransport().close();
    } catch (TException e) {
      throw new CodecException("Encoding Error:", e);
    }
    return bos.toByteArray();
  }

  @Override
  public S decode(byte[] data) throws CodecException {
    ByteArrayInputStream bis = new ByteArrayInputStream(data);
    TIOStreamTransport transport = new TIOStreamTransport(bis);
    TProtocol protocol = protocolFactory.getProtocol(transport);
    try {
      S typedInstance = clazz.newInstance();
      typedInstance.read(protocol);
      return typedInstance;
    } catch (InstantiationException | IllegalAccessException | TException e) {
      throw new CodecException("Decoding Error:", e);
    }
  }
}
