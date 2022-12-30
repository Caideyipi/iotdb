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
package com.timecho.timechodb.conf;

import org.apache.iotdb.db.audit.AuditLogOperation;
import org.apache.iotdb.db.audit.AuditLogStorage;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.external.api.IPropertiesLoader;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class TimechoDBDescriptor implements IPropertiesLoader {
  private static final Logger logger = LoggerFactory.getLogger(TimechoDBDescriptor.class);
  private static final String CONFIG_FILE = "iotdb-common.properties";

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  static final String RSA = "RSA";

  public static final String MAX_ALLOWED_TIME_SERIES_NUMBER = "max_allowed_time_series_number";

  private static final String PUBLIC_KEY_STR =
      "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAI4imMJhfHdZWWFZ3oYUwK8p04hiUgCUAgg8vyjSTdFJ2kQ-xD9ZQ8Goi_rIxICwwImJVPnZaOSLuYfH46XwIG8CAwEAAQ";

  private final Properties properties = new Properties();

  public TimechoDBDescriptor() {
    //    loadTimechoDBProperties();
  }

  @Override
  public Properties loadProperties() {
    return new Properties();
  }

  public void loadTimechoDBProperties() {
    Path file = ConfigFileLoader.getPropsUrl(CONFIG_FILE);
    if (file == null) {
      logger.warn("Couldn't load the configuration from any of the known sources.");
      return;
    }
    try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
      long fileSize = channel.size();
      if (fileSize >= Integer.MAX_VALUE) {
        logger.warn("We don't support jar's size larger than {}", Integer.MAX_VALUE);
      }
      ByteBuffer byteBuffer = ByteBuffer.allocate((int) fileSize);
      channel.read(byteBuffer);
      properties.load(new ByteArrayInputStream(byteBuffer.array()));
      config.setEnableWhiteList(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_white_list", String.valueOf(config.isEnableWhiteList()))));

      config.setEnableAuditLog(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_audit_log", String.valueOf(config.isEnableAuditLog()))));

      if (properties.getProperty("audit_log_storage") != null) {
        config.setAuditLogStorage(
            Arrays.stream(properties.getProperty("audit_log_storage").split(","))
                .map(AuditLogStorage::valueOf)
                .collect(Collectors.toList()));
      }

      if (properties.getProperty("audit_log_operation") != null) {
        config.setAuditLogOperation(
            Arrays.stream(properties.getProperty("audit_log_operation").split(","))
                .map(AuditLogOperation::valueOf)
                .collect(Collectors.toList()));
      }

      config.setEnableAuditLogForNativeInsertApi(
          Boolean.parseBoolean(
              properties.getProperty(
                  "enable_audit_log_for_native_insert_api",
                  String.valueOf(config.isEnableAuditLogForNativeInsertApi()))));
    } catch (Throwable t) {
      logger.warn("Error happened while loading properties from {}", file, t);
    }
  }

  @Override
  public Properties getCustomizedProperties() {
    // put the parameters that SeriesNumerLimiter used here.
    return properties;
  }

  private static PublicKey getPublicKey() throws NoSuchAlgorithmException, InvalidKeySpecException {
    KeyFactory keyFactory = KeyFactory.getInstance(RSA);
    X509EncodedKeySpec keySpec = new X509EncodedKeySpec(Base64.decodeBase64(PUBLIC_KEY_STR));
    return keyFactory.generatePublic(keySpec);
  }

  private static byte[] publicDecrypt(byte[] data, PublicKey publicKey)
      throws NoSuchPaddingException, IllegalBlockSizeException, NoSuchAlgorithmException,
          BadPaddingException, InvalidKeyException {
    try {
      Cipher cipher = Cipher.getInstance(RSA);
      cipher.init(Cipher.DECRYPT_MODE, publicKey);
      return cipher.doFinal(data);
    } catch (Throwable t) {
      logger.error("error happened while decrypt");
      throw t;
    }
  }

  public static TimechoDBDescriptor getInstance() {
    return TimechoDBDescriptorHolder.INSTANCE;
  }

  private static class TimechoDBDescriptorHolder {

    private static final TimechoDBDescriptor INSTANCE = new TimechoDBDescriptor();

    private TimechoDBDescriptorHolder() {}
  }
}
