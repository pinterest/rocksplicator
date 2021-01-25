/// Copyright 2021 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author Gopal Rajpurohit (grajpurohit@pinterest.com)
//

package com.pinterest.rocksplicator.codecs;

import com.google.common.base.Preconditions;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
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
