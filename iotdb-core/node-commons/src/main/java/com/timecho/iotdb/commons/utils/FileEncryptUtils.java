package com.timecho.iotdb.commons.utils;

import com.timecho.iotdb.commons.secret.SecretKey;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.encrypt.IEncryptor;

import java.io.IOException;

public class FileEncryptUtils {
  public static IEncryptor encryptor;
  public static IDecryptor decryptor;

  static {
    encryptor = SecretKey.getInstance().getEncryptor();
    decryptor = SecretKey.getInstance().getDecryptor();
  }

  public static byte[] encrypt(String content) throws IOException {
    return encryptor.encrypt(content.getBytes());
  }

  public static byte[] encrypt(byte[] bytes) throws IOException {
    return encryptor.encrypt(bytes);
  }

  public static byte[] decrypt(String content) throws IOException {
    return decryptor.decrypt(content.getBytes());
  }

  public static byte[] decrypt(byte[] bytes) throws IOException {
    return decryptor.decrypt(bytes);
  }
}
