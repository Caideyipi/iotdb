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
package com.timecho.iotdb.conf;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Properties;

public class TimechoDBDescriptor {
  private static final Logger logger = LoggerFactory.getLogger(TimechoDBDescriptor.class);
  private static final String CONFIG_FILE = "timechodb-common.properties";

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  static final String RSA = "RSA";

  public static final String MAX_ALLOWED_TIME_SERIES_NUMBER = "max_allowed_time_series_number";

  private static final String PUBLIC_KEY_STR =
      "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAI4imMJhfHdZWWFZ3oYUwK8p04hiUgCUAgg8vyjSTdFJ2kQ-xD9ZQ8Goi_rIxICwwImJVPnZaOSLuYfH46XwIG8CAwEAAQ";
  private final Properties properties = new Properties();

  public TimechoDBDescriptor() {
    //    loadTimechoDBProperties();
  }

  public void loadTimechoDBProperties(String configFile) {
    Path commonConfig = ConfigFileLoader.getPropsUrl(CONFIG_FILE);
    Path otherConfig = ConfigFileLoader.getPropsUrl(configFile);
    if (commonConfig == null) {
      logger.warn("Couldn't load the timechodb-common.properties from any of the known sources.");
      return;
    }

    if (otherConfig == null) {
      logger.warn("Couldn't load the " + configFile + " from any of the known sources.");
      return;
    }

    if (Files.exists(commonConfig)) {
      try (FileChannel commonChannel = FileChannel.open(commonConfig, StandardOpenOption.READ)) {
        long fileSize = commonChannel.size();
        if (fileSize >= Integer.MAX_VALUE) {
          logger.warn("We don't support commonConfig's size larger than {}", Integer.MAX_VALUE);
        }

        ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);
        PublicKey publicKey = getPublicKey();
        Cipher cipher = Cipher.getInstance(RSA);
        cipher.init(Cipher.DECRYPT_MODE, publicKey);
        PublicBAOS decryptedData = new PublicBAOS();
        if (fileSize > 0) {
          logger.info("load from timechodb-common.properties");
          decryptConfigFile(commonChannel, sizeBuffer, cipher, decryptedData);
        }
      } catch (Throwable t) {
        logger.warn("Error happened while loading properties from {}", commonConfig, t);
      }
    }

    if (Files.exists(otherConfig)) {
      try (FileChannel otherChannel = FileChannel.open(otherConfig, StandardOpenOption.READ)) {
        long fileSize = otherChannel.size();

        ByteBuffer sizeBuffer = ByteBuffer.allocate(Integer.BYTES);
        PublicKey publicKey = getPublicKey();
        Cipher cipher = Cipher.getInstance(RSA);
        cipher.init(Cipher.DECRYPT_MODE, publicKey);
        PublicBAOS decryptedData = new PublicBAOS();

        if (fileSize >= Integer.MAX_VALUE) {
          logger.warn("We don't support {}'s size larger than {}", configFile, Integer.MAX_VALUE);
        }

        decryptedData.reset();
        if (fileSize > 0) {
          logger.info("load from {}", configFile);
          decryptConfigFile(otherChannel, sizeBuffer, cipher, decryptedData);
        }
      } catch (Throwable t) {
        logger.warn("Error happened while loading properties from {}", commonConfig, t);
      }
    }
  }

  private void decryptConfigFile(
      FileChannel configChannel, ByteBuffer sizeBuffer, Cipher cipher, PublicBAOS decryptedData)
      throws IOException, IllegalBlockSizeException, BadPaddingException {
    while (configChannel.read(sizeBuffer) > 0) {
      sizeBuffer.flip();
      int size = sizeBuffer.getInt();
      sizeBuffer.flip();
      ByteBuffer encryptedData = ByteBuffer.allocate(size);
      configChannel.read(encryptedData);
      decryptedData.write(cipher.doFinal(encryptedData.array()));
    }
    logger.info(new String(decryptedData.getBuf(), 0, decryptedData.size()));
    properties.load(new ByteArrayInputStream(decryptedData.getBuf(), 0, decryptedData.size()));
  }

  public Properties getCustomizedProperties() {
    // put the parameters that SeriesNumerLimiter used here.
    return properties;
  }

  private static PublicKey getPublicKey() throws NoSuchAlgorithmException, InvalidKeySpecException {
    KeyFactory keyFactory = KeyFactory.getInstance(RSA);
    X509EncodedKeySpec keySpec = new X509EncodedKeySpec(Base64.decodeBase64(PUBLIC_KEY_STR));
    return keyFactory.generatePublic(keySpec);
  }

  public static TimechoDBDescriptor getInstance() {
    return TimechoDBDescriptorHolder.INSTANCE;
  }

  private static class TimechoDBDescriptorHolder {

    private static final TimechoDBDescriptor INSTANCE = new TimechoDBDescriptor();

    private TimechoDBDescriptorHolder() {}
  }
}
