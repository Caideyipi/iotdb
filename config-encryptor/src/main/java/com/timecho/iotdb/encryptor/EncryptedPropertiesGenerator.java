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
package com.timecho.iotdb.encryptor;

import org.apache.commons.net.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

public class EncryptedPropertiesGenerator {

  private static final String PRIVATE_KEY_STR =
      "MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAjiKYwmF8d1lZYVnehhTArynTiGJSAJQCCDy_KNJN0UnaRD7EP1lDwaiL-sjEgLDAiYlU-dlo5Iu5h8fjpfAgbwIDAQABAkEAgyOyk-4EO83pZKPZZxZwrWzG6hflFvl4YodBzHU1LToxKJZki4GtlIcxT6xhXHOWtCnm5LscC-J05sTvKkoO4QIhAO2qrg5gEmbI7SwAztzvQVL5QHx1gd2PEK6wc1PXdnQxAiEAmRlojZfRuXIVdvy9-v0mTo198dyNKCAUI3r7IzPN1p8CICJ-q1SazRDuCL5iP6QBddG9K4bk5zxpD1rLcXGxzBeBAiBPevBbabC4LHDWR9m8_kUvzKbQtCYX6adq0AKmwNMgkwIgbFZVDDCq15anFus10I10miW18ZsRzL2VgXaM1eEGd6o";

  private static final int ENCRYPT_MAX_SIZE = 32;

  static final String RSA = "RSA";

  public static void main(String[] args)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException,
          IllegalBlockSizeException, NoSuchPaddingException, BadPaddingException,
          InvalidKeyException {
    String rawFilePath = args[0];
    String encryptedFilePath = args[1];
    try (FileChannel reader = FileChannel.open(Paths.get(rawFilePath), StandardOpenOption.READ);
        FileChannel writer =
            FileChannel.open(
                Paths.get(encryptedFilePath),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
      long fileSize = reader.size();
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) fileSize);
      reader.read(byteBuffer);

      PrivateKey privateKey = getPrivateKey();

      Cipher cipher = Cipher.getInstance(RSA);
      cipher.init(Cipher.ENCRYPT_MODE, privateKey);
      byte[] rawData = byteBuffer.array();
      int offset = 0;
      while (offset < rawData.length) {
        int len = Math.min(ENCRYPT_MAX_SIZE, rawData.length - offset);
        byte[] encryptedData = cipher.doFinal(rawData, offset, len);
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + encryptedData.length);
        buffer.putInt(encryptedData.length);
        buffer.put(encryptedData);
        buffer.flip();
        writer.write(buffer);
        offset += len;
      }
    }
  }

  private static PrivateKey getPrivateKey()
      throws NoSuchAlgorithmException, InvalidKeySpecException {
    KeyFactory keyFactory = KeyFactory.getInstance(RSA);
    PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(Base64.decodeBase64(PRIVATE_KEY_STR));
    return keyFactory.generatePrivate(keySpec);
  }
}
