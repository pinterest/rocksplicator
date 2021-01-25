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

import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;

import com.google.common.base.Preconditions;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;

public class Codecs {

  private Codecs() {}

  public static <S> Codec<S, byte[]> getCompressedCodec(
      final Codec<S, byte[]> baseCodec,
      final CompressionAlgorithm compressionAlgorithm) {
    Preconditions.checkNotNull(compressionAlgorithm);
    Codec<S, byte[]> compressedCodec = null;
    switch (compressionAlgorithm) {
      case UNCOMPRESSED:
        compressedCodec = baseCodec;
        break;
      case GZIP:
        compressedCodec = new GZIPCompressionCodec<>(baseCodec);
        break;
      case SNAPPY:
        compressedCodec = new SnappyCompressionCodec<>(baseCodec);
        break;
      default:
        throw new RuntimeException(
            String.format("compression algorithm: %s not implemented", compressionAlgorithm));
    }
    return compressedCodec;
  }

  public static <S extends TBase<S, ?>> Codec<S, byte[]> createThriftCodec(
      final Class<S> thriftClazz,
      final SerializationProtocol serializationProtocol) {
    Preconditions.checkNotNull(thriftClazz);
    Preconditions.checkNotNull(serializationProtocol);
    ThriftCodec<S> baseCodec = null;
    switch (serializationProtocol) {
      case BINARY:
        baseCodec = new ThriftCodec<>(thriftClazz, new TBinaryProtocol.Factory());
        break;
      case COMPACT:
        baseCodec = new ThriftCodec<>(thriftClazz, new TCompactProtocol.Factory());
        break;
      default:
        throw new RuntimeException(
            String.format("serialization protocol : %s not implemented", serializationProtocol));
    }
    return baseCodec;
  }
}
