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
package com.timecho.timechodb.license;

import org.apache.commons.codec.binary.Base64;

import javax.crypto.Cipher;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;

public class Rsa {
  private static final String RSA_PUBLIC_KEY =
      "MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCAWzhK/X+CtDhMQyUzVEQH/euInXQxbC4vXPi/HNvhBqg4p4fGN600OFWpuB3ToGgL2wOP5+JVaOUqUh7GRSEUzzPt+GGOLGujuzsT5auZ/TgJmsWUTsL555bYYp5euoPBy/KdbkjMKR8vGTITG75ngj2JhN+1etYkUlgUfuhoOQIDAQAB";

  private static final String RSA_ALGORITHM_NO_PADDING = "RSA";
  public static final String RSA_ALGORITHM = "RSA/ECB/PKCS1Padding";

  private static final int MAX_ENCRYPT_BLOCK = 117;

  private static final int MAX_DECRYPT_BLOCK = 128;

  public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
  public static final String CIPHER_ENCRYPT = "encrypt";
  public static final String CIPHER_DECRYPT = "decrypt";

  private Rsa() {
    throw new IllegalStateException("Utility class");
  }

  private static PublicKey getPublicKey() throws NoSuchAlgorithmException, InvalidKeySpecException {
    X509EncodedKeySpec x509EncodedKeySpec =
        new X509EncodedKeySpec(Base64.decodeBase64(RSA_PUBLIC_KEY));
    KeyFactory keyFactory = KeyFactory.getInstance(RSA_ALGORITHM_NO_PADDING);
    return keyFactory.generatePublic(x509EncodedKeySpec);
  }

  public static String section(String type, String src, Cipher cipher) throws LicenseException {
    try {
      if (CIPHER_ENCRYPT.equals(type)) {
        byte[] bytes = src.getBytes(DEFAULT_CHARSET);
        int inputLen = bytes.length;
        int offSet = 0;
        byte[] cache;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int i = 0;
        while (inputLen - offSet > 0) {
          if (inputLen - offSet > MAX_ENCRYPT_BLOCK) {
            cache = cipher.doFinal(bytes, offSet, MAX_ENCRYPT_BLOCK);
          } else {
            cache = cipher.doFinal(bytes, offSet, inputLen - offSet);
          }
          out.write(cache, 0, cache.length);
          i++;
          offSet = i * MAX_ENCRYPT_BLOCK;
        }
        byte[] encryptedData = out.toByteArray();
        out.close();

        return Base64.encodeBase64String(encryptedData);
      } else if (CIPHER_DECRYPT.equals(type)) {
        byte[] bytes = Base64.decodeBase64(src);
        int inputLen = bytes.length;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int offSet = 0;
        byte[] cache;
        int i = 0;
        while (inputLen - offSet > 0) {
          if (inputLen - offSet > MAX_DECRYPT_BLOCK) {
            cache = cipher.doFinal(bytes, offSet, MAX_DECRYPT_BLOCK);
          } else {
            cache = cipher.doFinal(bytes, offSet, inputLen - offSet);
          }
          out.write(cache, 0, cache.length);
          i++;
          offSet = i * MAX_DECRYPT_BLOCK;
        }
        out.close();

        return out.toString();
      }
    } catch (Exception e) {
      throw new LicenseException("illegal license");
    }
    return "";
  }

  public static String publicEncrypt(String src) throws LicenseException {
    try {
      Cipher cipher = Cipher.getInstance(RSA_ALGORITHM);
      cipher.init(Cipher.ENCRYPT_MODE, getPublicKey());
      return section(CIPHER_ENCRYPT, src, cipher);
    } catch (Exception e) {
      throw new LicenseException("illegal license");
    }
  }

  public static String publicDecrypt(String src) throws LicenseException {
    try {
      Cipher cipher = Cipher.getInstance(RSA_ALGORITHM);
      cipher.init(Cipher.DECRYPT_MODE, getPublicKey());
      return section(CIPHER_DECRYPT, src, cipher);
    } catch (Exception e) {
      throw new LicenseException("illegal license");
    }
  }
}
