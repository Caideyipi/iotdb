package com.timecho.iotdb.commons.external.codec;

public interface BinaryDecoder extends Decoder {
  byte[] decode(byte[] var1) throws DecoderException;
}
