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
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.ByteArrayOutputStream;

public class ThriftStringEncoder<S extends TBase<S, ?>> implements Encoder<S, String> {

  private final TProtocolFactory protocolFactory;
  private final Class<S> thriftClazz;

  ThriftStringEncoder(Class<S> thriftClazz, TProtocolFactory protocolFactory) {
    this.protocolFactory = protocolFactory;
    this.thriftClazz = Preconditions.checkNotNull(thriftClazz);
  }

  public static <S extends TBase<S, ?>> ThriftStringEncoder<S> createToStringEncoder(
      Class<S> thriftClazz) {
    return new ThriftStringEncoder<>(thriftClazz, null);
  }

  public static <S extends TBase<S, ?>> ThriftStringEncoder<S> createJSONEncoder(
      Class<S> thriftClazz) {
    return new ThriftStringEncoder<>(thriftClazz, new TJSONProtocol.Factory());
  }

  public static <S extends TBase<S, ?>> ThriftStringEncoder<S> createSimpleJSONEncoder(
      Class<S> thriftClazz) {
    return new ThriftStringEncoder<>(thriftClazz, new TSimpleJSONProtocol.Factory());
  }

  @Override
  public String encode(S obj) throws CodecException {
    if (protocolFactory == null) {
      return obj.toString();
    }

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
