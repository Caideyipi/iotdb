package com.timecho.iotdb.commons.secret;

import com.timecho.iotdb.commons.encrypt.AES128.AES128Decryptor;
import com.timecho.iotdb.commons.encrypt.AES128.AES128Encryptor;
import com.timecho.iotdb.commons.encrypt.SM4128.SM4128Decryptor;
import com.timecho.iotdb.commons.encrypt.SM4128.SM4128Encryptor;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.encrypt.IEncryptor;
import org.apache.tsfile.file.metadata.enums.EncryptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;

public class SecretKey {
  private static final String SECRET_KEY_PATH = ".secretKey.bin";
  private static final String HARDWARE_CODE_PATH = ".hardware.bin";
  public static final String FILE_ENCRYPTED_SUFFIX = ".encrypted";
  public static final String FILE_ENCRYPTED_PREFIX = ".";
  private String secretKey;
  private String hardwareCode;
  private String encryptType;
  private IEncryptor encryptor;
  private IDecryptor decryptor;
  public static final Logger LOGGER = LoggerFactory.getLogger(SecretKey.class);

  public static SecretKey getInstance() {
    return SecretKeyHolder.INSTANCE;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setHardwareCode(String hardwareCode) {
    this.hardwareCode = hardwareCode;
  }

  public String getHardwareCode() {
    return hardwareCode;
  }

  public IEncryptor getEncryptor() {
    return encryptor;
  }

  public IDecryptor getDecryptor() {
    return decryptor;
  }

  public void initSecretKeyFile(String systemDir, String content) {
    if (content == null || content.isEmpty()) {
      return;
    }
    try {
      BinaryFileParser.writeStringAsBinary(
          systemDir + File.separator + SECRET_KEY_PATH, content, false, ByteOrder.BIG_ENDIAN);
      secretKey = content;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void initHardwareCodeFile(String systemDir, String content) {
    if (content == null || content.isEmpty()) {
      return;
    }
    try {
      BinaryFileParser.writeStringAsBinary(
          systemDir + File.separator + HARDWARE_CODE_PATH, content, false, ByteOrder.BIG_ENDIAN);
      hardwareCode = content;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void initEncryptProps(String encryptType) {
    this.encryptType = encryptType;
    if (encryptType.contains(EncryptionType.AES128.getExtension())) {
      encryptor = new AES128Encryptor(getRealSecretKey());
      decryptor = new AES128Decryptor(getRealSecretKey());
    } else if (encryptType.contains(EncryptionType.SM4128.getExtension())) {
      encryptor = new SM4128Encryptor(getRealSecretKey());
      decryptor = new SM4128Decryptor(getRealSecretKey());
    }
  }

  public void loadSecretKeyFromFile(String systemDir) throws IOException {
    File file = new File(systemDir + File.separator + SECRET_KEY_PATH);
    if (!file.exists()) {
      LOGGER.error("Secret key is not ready.");
      throw new IOException("Secret key is not ready.");
    }

    String content =
        BinaryFileParser.readStringFromBinary(
            systemDir + File.separator + SECRET_KEY_PATH, ByteOrder.BIG_ENDIAN);
    if (content.isEmpty()) {
      LOGGER.error("Secret key is not ready.");
      throw new IOException("Secret key is not ready.");
    }
    this.secretKey = content;
  }

  public void loadHardwareCodeFromFile(String systemDir) throws IOException {
    File file = new File(systemDir + File.separator + HARDWARE_CODE_PATH);
    if (!file.exists()) {
      LOGGER.error("Hardware code is not ready.");
      throw new IOException("Hardware code is not ready.");
    }

    String content =
        BinaryFileParser.readStringFromBinary(
            systemDir + File.separator + HARDWARE_CODE_PATH, ByteOrder.BIG_ENDIAN);
    if (content.isEmpty()) {
      LOGGER.error("Hardware code is not ready.");
      throw new IOException("Hardware code is not ready.");
    }
    this.hardwareCode = content;
  }

  public byte[] getRealSecretKey() {
    return EncryptUtils.getEncryptKeyFromToken(secretKey, hardwareCode.getBytes());
  }

  private static class SecretKeyHolder {

    private static final SecretKey INSTANCE = new SecretKey();

    private SecretKeyHolder() {}
  }
}
