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
