package com.timecho.iotdb.commons.utils;

import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.commons.structure.SortedProperties;

import com.google.common.collect.ImmutableMap;
import com.timecho.iotdb.commons.commission.Lottery;
import com.timecho.iotdb.commons.systeminfo.ISystemInfoGetter;
import com.timecho.iotdb.commons.systeminfo.LinuxSystemInfoGetter;
import com.timecho.iotdb.commons.systeminfo.MacSystemInfoGetter;
import com.timecho.iotdb.commons.systeminfo.SystemInfoGetter;
import com.timecho.iotdb.commons.systeminfo.WindowsSystemInfoGetter;
import org.apache.commons.codec.binary.Base32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.function.Supplier;

public class OSUtils {
  private static final Logger logger = LoggerFactory.getLogger(OSUtils.class);
  private static final String SYSTEM_INFO_VERSION = "02";
  private static final ISystemInfoGetter systemInfoGetter = generateSystemInfoGetter();
  public static final ImmutableMap<String, Supplier<String>> hardwareSystemInfoNameToItsGetter =
      ImmutableMap.of(
          Lottery.CPU_ID_NAME, systemInfoGetter::getCPUId,
          Lottery.MAIN_BOARD_ID_NAME, systemInfoGetter::getMainBoardId,
          Lottery.SYSTEM_UUID_NAME, systemInfoGetter::getSystemUUID);

  public static SystemInfoGetter generateSystemInfoGetter() {
    String os = System.getProperty("os.name").toLowerCase();
    if (os.contains("windows")) {
      return new WindowsSystemInfoGetter();
    } else if (os.contains("mac")) {
      return new MacSystemInfoGetter();
    } else if (os.contains("linux")) {
      return new LinuxSystemInfoGetter();
    }
    logger.warn("OS {} is not officially supported, will be treated as Linux", os);
    return new LinuxSystemInfoGetter();
  }

  public static String generateSystemInfoContentWithVersion() throws LicenseException {
    return SYSTEM_INFO_VERSION + "-" + generateSystemInfoContentOfV02(getPropertiesList());
  }

  private static List<Properties> getPropertiesList() {
    Properties hardwareInfoProperties = new SortedProperties();
    OSUtils.hardwareSystemInfoNameToItsGetter.forEach(
        (name, getter) -> hardwareInfoProperties.setProperty(name, getter.get()));
    return Arrays.asList(hardwareInfoProperties, new SortedProperties(), new SortedProperties());
  }

  public static String generateSystemInfoContentOfV02(List<Properties> propertiesList)
      throws LicenseException {
    try {
      List<String> encodedList = new ArrayList<>();
      for (Properties properties : propertiesList) {
        StringWriter stringWriter = new StringWriter();
        properties.store(stringWriter, null);
        String originalInfo = stringWriter.toString();
        // remove time
        if (originalInfo.charAt(0) == '#') {
          originalInfo = originalInfo.substring(originalInfo.indexOf('\n') + 1);
        }
        encodedList.add(OSUtils.encodeTo8(originalInfo));
      }
      return encodedList.stream().reduce((a, b) -> a + "-" + b).get();
    } catch (Exception e) {
      throw new LicenseException(e);
    }
  }

  public static String encodeTo8(String originalInfo) throws NoSuchAlgorithmException {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    Base32 base32 = new Base32();
    return base32.encodeAsString(md5.digest(originalInfo.getBytes())).substring(0, 8);
  }
}
