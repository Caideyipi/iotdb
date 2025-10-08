/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.timecho.iotdb.commons.encrypt.SM4128;

import org.apache.tsfile.encrypt.IEncryptor;
import org.apache.tsfile.exception.encrypt.EncryptKeyLengthNotMatchException;
import org.apache.tsfile.file.metadata.enums.EncryptionType;

import java.util.Arrays;

public class SM4128Encryptor implements IEncryptor {

  private final SM4Utils sm4;

  public SM4128Encryptor(byte[] key) {
    if (key.length != 16) {
      throw new EncryptKeyLengthNotMatchException(16, key.length);
    }
    this.sm4 = new SM4Utils(key, key);
  }

  @Override
  public byte[] encrypt(byte[] data) {
    return sm4.cryptData_CTR(data);
  }

  @Override
  public byte[] encrypt(byte[] data, int offset, int size) {
    return encrypt(Arrays.copyOfRange(data, offset, offset + size));
  }

  @Override
  public EncryptionType getEncryptionType() {
    return EncryptionType.SM4128;
  }
}
