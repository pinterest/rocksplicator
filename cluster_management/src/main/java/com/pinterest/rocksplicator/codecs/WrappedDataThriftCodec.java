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

  public WrappedDataThriftCodec(
      final Class<S> thriftClazz,
      final SerializationProtocol serializationProtocol,
      final CompressionAlgorithm compressionAlgorithm) {
    this.serializationProtocol = Preconditions.checkNotNull(serializationProtocol);
    this.compressionAlgorithm = Preconditions.checkNotNull(compressionAlgorithm);
    this.thriftClazz = Preconditions.checkNotNull(thriftClazz);
    this.wrappedDataBinaryCodec =
        Codecs.createThriftCodec(WrappedData.class, SerializationProtocol.BINARY);

    ImmutableMap.Builder<SerializationProtocol, Map<CompressionAlgorithm, Codec<S, byte[]>>>
        allCodecsPairsBuilder =
        ImmutableMap.builder();
    for (SerializationProtocol protocol : SerializationProtocol.values()) {
      Codec<S, byte[]> thriftCodec = Codecs.createThriftCodec(thriftClazz, protocol);

      ImmutableMap.Builder<CompressionAlgorithm, Codec<S, byte[]>>
          compressedCodecsBuilder =
          ImmutableMap.builder();
      for (CompressionAlgorithm algorithm : CompressionAlgorithm.values()) {
        Codec<S, byte[]> compressedCodec =
            Codecs.getCompressedCodec(thriftCodec, algorithm);
        compressedCodecsBuilder.put(algorithm, compressedCodec);
      }
      allCodecsPairsBuilder.put(protocol, compressedCodecsBuilder.build());
    }
    this.allCodecsPairs = allCodecsPairsBuilder.build();
  }

  @Override
  public byte[] encode(S obj) throws CodecException {
    WrappedData wrappedData = new WrappedData();
    Codec<S, byte[]>
        codec =
        this.allCodecsPairs.get(this.serializationProtocol).get(this.compressionAlgorithm);
    wrappedData.setSerialization_protocol(this.serializationProtocol)
        .setCompression_algorithm(this.compressionAlgorithm)
        .setData(codec.encode(obj));
    return this.wrappedDataBinaryCodec.encode(wrappedData);
  }

  @Override
  public S decode(byte[] data) throws CodecException {
    WrappedData wrappedData = wrappedDataBinaryCodec.decode(data);
    Codec<S, byte[]>
        codec =
        this.allCodecsPairs.get(wrappedData.getSerialization_protocol())
            .get(wrappedData.getCompression_algorithm());
    return codec.decode(wrappedData.getData());
  }
}
