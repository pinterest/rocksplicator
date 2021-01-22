package com.pinterest.rocksplicator.codecs;

import com.pinterest.rocksplicator.thrift.commons.io.CompressionAlgorithm;
import com.pinterest.rocksplicator.thrift.commons.io.SerializationProtocol;
import com.pinterest.rocksplicator.thrift.commons.io.WrappedData;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.thrift.TBase;

import java.util.Map;

public class WrappedDataThriftCodec<S extends TBase<S, ?>> implements Codec<S, byte[]> {

  final Class<S> thriftClazz;
  final SerializationProtocol serializationProtocol;
  final CompressionAlgorithm compressionAlgorithm;
  final Map<SerializationProtocol, Map<CompressionAlgorithm, Codec<S, byte[]>>> allCodecsPairs;
  private Codec<WrappedData, byte[]> wrappedDataBinaryCodec;

  private WrappedDataThriftCodec(
      final Class<S> thriftClazz,
      final SerializationProtocol serializationProtocol,
      final CompressionAlgorithm compressionAlgorithm) {
    this.serializationProtocol = Preconditions.checkNotNull(serializationProtocol);
    this.compressionAlgorithm = Preconditions.checkNotNull(compressionAlgorithm);
    this.thriftClazz = Preconditions.checkNotNull(thriftClazz);
    this.wrappedDataBinaryCodec = ThriftCodec.<WrappedData>createBinaryCodec(WrappedData.class);

    ImmutableMap.Builder<SerializationProtocol, Map<CompressionAlgorithm, Codec<S, byte[]>>>
        allCodecsPairsBuilder =
        ImmutableMap.builder();
    for (SerializationProtocol sProto : SerializationProtocol.values()) {
      Codec<S, byte[]> baseCodec = null;
      switch (sProto) {
        case BINARY:
          baseCodec = ThriftCodec.<S>createBinaryCodec(thriftClazz);
          break;
        case COMPACT:
          baseCodec = ThriftCodec.<S>createCompactCodec(thriftClazz);
          break;
        default:
          throw new RuntimeException(
              String.format("serialization protocol : %s not implemented", sProto));
      }
      ImmutableMap.Builder<CompressionAlgorithm, Codec<S, byte[]>>
          compressionCodecBuilder =
          ImmutableMap.builder();

      for (CompressionAlgorithm algorithm : CompressionAlgorithm.values()) {
        Codec<S, byte[]> compressedCodec = null;
        switch (algorithm) {
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
                String.format("compression algorithm: %s not implemented", algorithm));
        }
        compressionCodecBuilder.put(algorithm, compressedCodec);
      }
      allCodecsPairsBuilder.put(sProto, compressionCodecBuilder.build());
    }
    this.allCodecsPairs = allCodecsPairsBuilder.build();
  }

  @Override
  public byte[] encode(S obj) throws CodecException {
    WrappedData wrappedData = new WrappedData();
    wrappedData
    return this.wrappedDataBinaryCodec.encode(wrappedData);
  }

  @Override
  public S decode(byte[] data) throws CodecException {
    WrappedData wrappedData = wrappedDataBinaryCodec.decode(data);

    return null;
  }
}
