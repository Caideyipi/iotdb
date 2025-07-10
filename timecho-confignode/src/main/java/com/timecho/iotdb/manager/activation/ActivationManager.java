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

package com.timecho.iotdb.manager.activation;

import org.apache.iotdb.common.rpc.thrift.TLicense;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.commons.license.ActivateStatus;
import org.apache.iotdb.commons.structure.SortedProperties;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.TClusterActivationStatus;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import cn.hutool.system.OsInfo;
import cn.hutool.system.SystemUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.timecho.iotdb.manager.TimechoConfigManager;
import com.timecho.iotdb.manager.activation.systeminfo.ISystemInfoGetter;
import com.timecho.iotdb.manager.activation.systeminfo.LinuxSystemInfoGetter;
import com.timecho.iotdb.manager.activation.systeminfo.MacSystemInfoGetter;
import com.timecho.iotdb.manager.activation.systeminfo.SystemInfoGetter;
import com.timecho.iotdb.manager.activation.systeminfo.WindowsSystemInfoGetter;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.ratis.util.AutoCloseableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.NODE_UUID_IN_ENV_FILE;

public class ActivationManager {
  static final Logger logger = LoggerFactory.getLogger(ActivationManager.class);

  private final TimechoConfigManager configManager;

  private static final String SYSTEM_INFO_VERSION = "02";
  private static final int VERSION_LENGTH = 2;

  private static final String CONFIGNODE_HOME_PATH =
      System.getProperty("CONFIGNODE_HOME") == null ? "." : System.getProperty("CONFIGNODE_HOME");
  public static final String ACTIVATION_DIR_PATH =
      CONFIGNODE_HOME_PATH + File.separatorChar + "activation";
  public static final String LICENSE_FILE_NAME = "license";
  public static final String LICENSE_FILE_PATH =
      ACTIVATION_DIR_PATH + File.separatorChar + LICENSE_FILE_NAME;
  public static final String SYSTEM_INFO_FILE_PATH =
      ACTIVATION_DIR_PATH + File.separatorChar + "system_info";
  public static final String HISTORY_FILE_PATH =
      ACTIVATION_DIR_PATH + File.separatorChar + ".history";
  public static final String ENV_FILE_PATH =
      CONFIGNODE_HOME_PATH + File.separatorChar + IoTDBConstant.ENV_FILE_NAME;

  // region Time
  private static final long ONE_SECOND = TimeUnit.SECONDS.toMillis(1);
  private static final long ONE_MINUTE = TimeUnit.MINUTES.toMillis(1);
  private static final long ONE_HOUR = TimeUnit.HOURS.toMillis(1);
  private static final long ONE_DAY = TimeUnit.DAYS.toMillis(1);
  private static final long ONE_MONTH = 30 * ONE_DAY;

  public static final long FILE_MONITOR_INTERVAL = ONE_SECOND;
  private static final long LEADER_DISCONNECT_FROM_ACTIVE_NODE_TIME_LIMIT = 5 * ONE_SECOND;
  private static final long LICENSE_MANAGER_PERIODICAL_TASK_MINIMAL_INTERVAL = ONE_SECOND;
  private static final long LICENSE_MANAGER_PERIODICAL_TASK_MAXIMAL_INTERVAL = ONE_MINUTE;

  // endregion

  private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  ExecutorService executor =
      IoTDBThreadPoolFactory.newFixedThreadPool(2, ThreadName.ACTIVATION_SERVICE.getName());

  protected License license;

  private final AtomicLong lastTimeHeardActiveNode = new AtomicLong(0);

  LicenseFileMonitor licenseFileMonitor;

  private static final ISystemInfoGetter systemInfoGetter = generateSystemInfoGetter();

  static final ImmutableMap<String, Supplier<String>> hardwareSystemInfoNameToItsGetter =
      ImmutableMap.of(
          License.CPU_ID_NAME, systemInfoGetter::getCPUId,
          License.MAIN_BOARD_ID_NAME, systemInfoGetter::getMainBoardId,
          License.SYSTEM_UUID_NAME, systemInfoGetter::getSystemUUID);

  static final ImmutableMap<String, Supplier<String>> configurableSystemInfoNameToItsGetter =
      ImmutableMap.of(
          License.IP_ADDRESS_NAME,
          () -> ConfigNodeDescriptor.getInstance().getConf().getInternalAddress(),
          License.INTERNAL_PORT_NAME,
          () -> String.valueOf(ConfigNodeDescriptor.getInstance().getConf().getInternalPort()),
          License.IS_SEED_CONFIGNODE_NODE_NAME,
          () -> String.valueOf(ConfigNodeDescriptor.getInstance().isSeedConfigNode()));

  private static final ImmutableMap<String, Supplier<String>> systemInfoNameToItsGetter =
      ImmutableMap.<String, Supplier<String>>builder()
          .putAll(hardwareSystemInfoNameToItsGetter)
          .putAll(configurableSystemInfoNameToItsGetter)
          .build();

  /** In some situation (like not root user) we cannot get cpu id or main board id. */
  private final ImmutableSet<String> systemInfoAllowEmpty =
      ImmutableSet.of(License.CPU_ID_NAME, License.MAIN_BOARD_ID_NAME, License.SYSTEM_UUID_NAME);

  ReentrantLock loadLock = new ReentrantLock();

  public ActivationManager(TimechoConfigManager configManager) throws LicenseException {
    this.configManager = configManager;

    initLicense();

    try {
      generateNodeUUIDIfNotExist();
    } catch (IOException e) {
      throw new LicenseException(e);
    }

    createActivationDir();

    tryLoadLicenseForTheFirstTime();

    launchLicenseFileMonitorService();

    executor.submit(this::activeNodeMonitorService);
    logger.info("Active node watching service launched successfully");

    executor.submit(this::expirationWarningService);
    logger.info("Expiration warning service launched successfully");
  }

  private void initLicense() {
    this.license =
        new License(
            () ->
                configManager
                    .getClusterSchemaManager()
                    .updateSchemaQuotaConfiguration(
                        license.getDeviceNumLimit(), license.getSensorNumLimit()));
  }

  static SystemInfoGetter generateSystemInfoGetter() {
    OsInfo osInfo = SystemUtil.getOsInfo();
    if (osInfo.isWindows()) {
      return new WindowsSystemInfoGetter();
    } else if (osInfo.isMac()) {
      return new MacSystemInfoGetter();
    } else if (osInfo.isLinux()) {
      return new LinuxSystemInfoGetter();
    }
    logger.warn("OS {} is not officially supported, will be treated as Linux", osInfo.getName());
    return new LinuxSystemInfoGetter();
  }

  private void createActivationDir() throws LicenseException {
    File activationDir = new File(ACTIVATION_DIR_PATH);
    if (!activationDir.exists() || !activationDir.isDirectory()) {
      logger.info(
          "try to make activation dir {}, absolute path {}",
          ACTIVATION_DIR_PATH,
          activationDir.getAbsolutePath());
      boolean makeDirSuccess = activationDir.mkdir();
      if (!makeDirSuccess) {
        final String msg =
            String.format("failed to create activation dir at %s", activationDir.getAbsolutePath());
        logger.error(msg);
        throw new LicenseException(msg);
      }
      logger.info("successfully create activation dir at {}", activationDir.getAbsolutePath());
    }
  }

  private void tryLoadLicenseForTheFirstTime() {
    File file = new File(LICENSE_FILE_PATH);
    if (file.exists()) {
      logger.info("License file detected during ConfigNode's starting.");
      tryLoadLicenseFromFile();
    } else {
      logger.info("License file not detected during ConfigNode's starting.");
    }
  }

  private void launchLicenseFileMonitorService() throws LicenseException {
    licenseFileMonitor = new LicenseFileMonitor();
    licenseFileMonitor.monitor(ACTIVATION_DIR_PATH, new LicenseFileAlterationListener());
    try {
      licenseFileMonitor.start();
    } catch (Exception e) {
      logger.error("start licenseFileMonitor fail");
      throw new LicenseException(e);
    }
    logger.info("License file watching service launched successfully");
  }

  private class LicenseFileAlterationListener extends FileAlterationListenerAdaptor {
    @Override
    public void onFileCreate(File file) {
      if (LICENSE_FILE_NAME.equals(file.getName())) {
        logger.info("license file creation detected");
        tryLoadLicenseFromFile();
      }
    }

    @Override
    public void onFileChange(File file) {
      if (LICENSE_FILE_NAME.equals(file.getName())) {
        logger.info("license file modification detected");
        tryLoadLicenseFromFile();
      }
    }

    @Override
    public void onFileDelete(File file) {
      if (LICENSE_FILE_NAME.equals(file.getName())) {
        logger.info("license file deletion detected");
        license.licenseFileNotExistOrInvalid();
        license.logActivateStatus(false);
      }
    }
  }

  private class LicenseFileMonitor {
    private final FileAlterationMonitor monitor;

    public LicenseFileMonitor() {
      monitor = new FileAlterationMonitor(FILE_MONITOR_INTERVAL);
    }

    public void monitor(String path, LicenseFileAlterationListener listener) {
      FileAlterationObserver observer = new FileAlterationObserver(new File(path));
      monitor.addObserver(observer);
      observer.addListener(listener);
    }

    public void stop() throws LicenseException {
      try {
        monitor.stop();
      } catch (Exception e) {
        throw new LicenseException(e);
      }
    }

    public void start() throws LicenseException {
      try {
        monitor.start();
      } catch (Exception e) {
        throw new LicenseException(e);
      }
    }
  }

  /*
   activationManagerPeriodicalTask now do two things:
   1. Check active node existence every second.
      If I've not heard any Active ConfigNode for more than disconnectionFromActiveNodeTimeLimit ms,
      set myself to unactivated state.
   2. Check how much time left before the license expiration. If remain time is less than a week, warn in log.
  */
  private void activeNodeMonitorService() {
    long lastTimeWarnDisconnection = 0;
    while (true) {
      try (AutoCloseableLock ignore = AutoCloseableLock.acquire(loadLock)) {
        checkSystemTimeAndIssueTime();
        long now = System.currentTimeMillis();
        if (this.license.isActive()) {
          this.lastTimeHeardActiveNode.set(System.currentTimeMillis());
        }
        final long disconnectionLimit = license.getDisconnectionFromActiveNodeTimeLimit();
        if (this.license.isActivated()) {
          // Active node related task
          long timeRemain = passiveActiveTimeRemain();
          final String disconnectionStr =
              CommonDateTimeUtils.convertMillisecondToDurationStr(msSinceLastTimeHeardActiveNode());
          final String remainStr =
              CommonDateTimeUtils.convertMillisecondToDurationStr(passiveActiveTimeRemain());
          final String disconnectionLimitStr =
              CommonDateTimeUtils.convertMillisecondToDurationStr(disconnectionLimit);
          if (timeRemain < 0) {
            logger.warn(
                "This ConfigNode has disconnected from all active ConfigNodes for {} ({} ms), exceeds the disconnection time limit {}. License will be given up now.",
                disconnectionStr,
                msSinceLastTimeHeardActiveNode(),
                disconnectionLimitStr);
            giveUpLicense("active node disconnection time limit exceeded");
          } else if (disconnectionLimit * 0.9 < timeRemain) {
            // Has good connection with active node, or I'm an active node. Do nothing.
          } else if (timeRemain < ONE_HOUR) {
            // Remaining time is less than 1 hour, warning every minute.
            if (now - lastTimeWarnDisconnection > ONE_MINUTE) {
              disconnectionWarn(remainStr);
              lastTimeWarnDisconnection = now;
            }
          } else if (ONE_HOUR < timeRemain && timeRemain < ONE_DAY) {
            // Remaining time is less than 1 day. Warning every hour.
            if (now - lastTimeWarnDisconnection > ONE_HOUR) {
              disconnectionWarn(remainStr);
              lastTimeWarnDisconnection = now;
            }
          } else {
            // Remaining time is more than 1 day. Warning every day.
            if (now - lastTimeWarnDisconnection > ONE_DAY) {
              disconnectionWarn(remainStr);
              lastTimeWarnDisconnection = now;
            }
          }
        }
      }
      try {
        long sleepInterval = license.getDisconnectionFromActiveNodeTimeLimit() / 10;
        // sleep interval shall not be too long or too short
        sleepInterval = Math.min(sleepInterval, LICENSE_MANAGER_PERIODICAL_TASK_MAXIMAL_INTERVAL);
        sleepInterval = Math.max(sleepInterval, LICENSE_MANAGER_PERIODICAL_TASK_MINIMAL_INTERVAL);
        Thread.sleep(sleepInterval);
      } catch (InterruptedException e) {
        logger.warn("Sleeping was interrupted", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private void expirationWarningService() {
    long lastTimeWarn = 0;
    while (true) {
      final long now = System.currentTimeMillis();
      final long timeRemain = license.getLicenseExpireTimestamp() - now;
      if (license.getLicenseExpireTimestamp() == 0) {
        if (now - lastTimeWarn > ONE_HOUR) {
          logger.warn(
              "License has not been set, and this ConfigNode currently not connects to any active ConfigNode. Cluster is readonly now. Contact Timecho for more information.");
          lastTimeWarn = now;
        }
      } else if (timeRemain < 0) {
        final String expireTimeString =
            DateTimeUtils.convertLongToDate(license.getLicenseExpireTimestamp(), "ms");
        logger.warn(
            "License expired at {}, cluster is readonly now. Contact Timecho for more information.",
            expireTimeString);
      } else if (timeRemain < ONE_HOUR) {
        expirationWarn(now);
        lastTimeWarn = now;
      } else if (timeRemain < ONE_DAY) {
        if (now - lastTimeWarn > ONE_HOUR) {
          expirationWarn(now);
          lastTimeWarn = now;
        }
      } else if (timeRemain < ONE_MONTH) {
        if (now - lastTimeWarn > ONE_DAY) {
          expirationWarn(now);
          lastTimeWarn = now;
        }
      }
      try {
        Thread.sleep(ONE_MINUTE);
      } catch (InterruptedException e) {
        logger.warn("Sleeping was interrupted", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  private void disconnectionWarn(String timeRemainStr) {
    logger.warn(
        "Disconnect from all active ConfigNode, will switch to read-only mode in {}",
        timeRemainStr);
  }

  private void expirationWarn(Long now) {
    String expirationTimeStr =
        DateTimeUtils.convertLongToDate(license.getLicenseExpireTimestamp(), "ms");
    String timeRemainStr =
        CommonDateTimeUtils.convertMillisecondToDurationStr(
            license.getLicenseExpireTimestamp() - now);
    logger.warn(
        "License will expire at {}, there is {} left. Cluster will only allow reading when the time comes. Contact Timecho for more information.",
        expirationTimeStr,
        timeRemainStr);
  }

  // If in passive activated state, calculate how much time still remain
  public long passiveActiveTimeRemain() {
    return license.getDisconnectionFromActiveNodeTimeLimit() - msSinceLastTimeHeardActiveNode();
  }

  protected void tryLoadLicenseFromFile() {
    try (FileReader fileReader = new FileReader(LICENSE_FILE_PATH);
        AutoCloseableLock ignore = AutoCloseableLock.acquire(loadLock)) {
      StringBuilder builder = new StringBuilder();
      int data;
      while ((data = fileReader.read()) != -1) {
        builder.append((char) data);
      }
      final String encryptedLicenseContent = builder.toString();
      Properties licenseProperties = loadLicenseFromEveryVersion(encryptedLicenseContent);
      if (!verifyAllSystemInfo(licenseProperties)) {
        throw new LicenseException("This license is not allowed to activate this ConfigNode.");
      }
      if (license.loadFromProperties(licenseProperties, true)) {
        logger.info("Load license success.");
      }
      checkSystemTimeAndIssueTime();
    } catch (Exception e) {
      logger.error("Load license fail.", e);
      license.licenseFileNotExistOrInvalid();
    }
  }

  public boolean checkLicenseContentAvailable(String encryptedLicenseContent) {
    try {
      Properties licenseProperties = loadLicenseFromEveryVersion(encryptedLicenseContent);
      return verifyAllSystemInfo(licenseProperties);
    } catch (Exception e) {
      return false;
    }
  }

  protected Properties loadLicenseFromEveryVersion(String encryptedLicenseContent)
      throws LicenseException, IOException {
    return ActivationManager.loadLicenseFromEveryVersionStatic(encryptedLicenseContent);
  }

  protected static Properties loadLicenseFromEveryVersionStatic(String encryptedLicenseContent)
      throws LicenseException, IOException {
    logger.info("Loading license: \n{}", encryptedLicenseContent);
    String licenseVersion = checkLicenseVersion(encryptedLicenseContent);
    String deprecatedLicenseContent;
    if ("00".equals(licenseVersion)) {
      deprecatedLicenseContent = decryptV00(encryptedLicenseContent);
    } else if ("01".equals(licenseVersion)) {
      deprecatedLicenseContent = decryptV01(encryptedLicenseContent);
    } else {
      throw new LicenseException("license version " + licenseVersion + " is not supported");
    }
    Properties licenseProperties = new Properties();
    licenseProperties.load(new StringReader(deprecatedLicenseContent));
    return licenseProperties;
  }

  private static String checkLicenseVersion(String encryptedLicenseContent) {
    if (encryptedLicenseContent.charAt(2) != '-') {
      return "00";
    }
    return encryptedLicenseContent.substring(0, 2);
  }

  @TestOnly
  public static void tryLoadLicenseFromString(License license, String licenseContent)
      throws IOException, LicenseException {
    Properties properties = new Properties();
    properties.load(new StringReader(licenseContent));
    license.loadFromProperties(properties, true);
  }

  public void tryLoadRemoteLicense(TLicense remoteLicense) {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(loadLock)) {
      if (remoteLicense == null) {
        // do nothing
      } else if (remoteLicense.licenseIssueTimestamp < this.license.getLicenseIssueTimestamp()) {
        // remote license is older, do nothing
        final String myIssueTime = dateFormat.format(this.license.getLicenseIssueTimestamp());
        final String remoteIssueTime = dateFormat.format(remoteLicense.licenseIssueTimestamp);
        logger.info(
            "Receive remote license which issue timestamp {} ({}) is older than mine {} ({}), ignored",
            remoteIssueTime,
            remoteLicense.licenseIssueTimestamp,
            myIssueTime,
            this.license.getLicenseIssueTimestamp());
      } else if (remoteLicense.licenseIssueTimestamp == this.license.getLicenseIssueTimestamp()) {
        // remote license is the same, just update my lastTimeHeardActiveNode
        this.heardActiveNode();
      } else {
        logger.info("Loading remote license...");
        this.heardActiveNode();
        try {
          this.license.loadFromTLicense(remoteLicense);
        } catch (LicenseException e) {
          logger.error("Loading remote license fail.", e);
          return;
        }
        logger.info("License updated because receive remote license");
        checkSystemTimeAndIssueTime();
      }
    }
  }

  public void giveUpLicense(String reason) {
    if (license.reset()) {
      logger.warn("License has been given up, because {}", reason);
    }
  }

  public void giveUpLicenseBecauseLeaderBelieveThereIsNoActiveNodeInCluster() {
    try (AutoCloseableLock ignore = AutoCloseableLock.acquire(loadLock)) {
      if (!license.isActive()) {
        giveUpLicense("there is no active node in cluster");
      }
    }
  }

  public License getLicense() {
    return this.license;
  }

  public TLicense getLicenseUsage() {
    TLicense usage = this.license.toTLicense();
    usage
        .setDataNodeNum((short) configManager.getNodeManager().getRegisteredDataNodeCount())
        .setCpuCoreNum(configManager.getNodeManager().getDataNodeCpuCoreCount())
        .setDeviceNum(configManager.getClusterSchemaManager().getDeviceUsage())
        .setSensorNum(configManager.getClusterSchemaManager().getTimeSeriesUsage())
        .setAiNodeNum((short) configManager.getNodeManager().getRegisteredAINodeInfoList().size());
    return usage;
  }

  public TClusterActivationStatus calculateClusterActivationStatus() {
    Map<Integer, ActivateStatus> activationMap =
        configManager.getLoadManager().getNodeActivateStatus().entrySet().stream()
            .collect(
                Collectors.toMap(Entry::getKey, entry -> ActivateStatus.valueOf(entry.getValue())));
    Collection<ActivateStatus> activateStatuses = activationMap.values();
    if (activateStatuses.stream().allMatch(ActivateStatus::isFullyActivated)) {
      return TClusterActivationStatus.ACTIVATED;
    } else if (activateStatuses.stream().allMatch(ActivateStatus::isActivated)) {
      return TClusterActivationStatus.PARTLY_ACTIVATED;
    } else if (activateStatuses.stream().anyMatch(ActivateStatus::isUnactivated)) {
      return TClusterActivationStatus.UNACTIVATED;
    } else if (activateStatuses.stream().anyMatch(ActivateStatus::isActivated)) {
      return TClusterActivationStatus.PARTLY_ACTIVATED;
    } else {
      return TClusterActivationStatus.UNKNOWN;
    }
  }

  public ActivateStatus getActivateStatus() {
    return license.getActivateStatus();
  }

  public boolean isActivated() {
    if (!this.license.isActivated()) {
      return false;
    }
    if (this.license.getDataNodeNumLimit()
        < configManager.getNodeManager().getRegisteredDataNodeCount()) {
      return false;
    }
    if (this.license.getDataNodeCpuCoreNumLimit()
        < configManager.getNodeManager().getDataNodeCpuCoreCount()) {
      return false;
    }
    return true;
  }

  public boolean isActive() {
    return this.license.isActive();
  }

  public void heardActiveNode() {
    this.lastTimeHeardActiveNode.set(System.currentTimeMillis());
  }

  /**
   * Leader only broadcast license when it knows there are active nodes in cluster. This detection
   * needs to be sensitive. This method should only be use when leader needs to send heartbeat req.
   *
   * @return Whether active node exists
   */
  public boolean activeNodeExistForLeader() {
    return msSinceLastTimeHeardActiveNode() <= LEADER_DISCONNECT_FROM_ACTIVE_NODE_TIME_LIMIT;
  }

  private long msSinceLastTimeHeardActiveNode() {
    return System.currentTimeMillis() - lastTimeHeardActiveNode.get();
  }

  /**
   * Generate IOTDB_HOME/.env if file not exists, set node UUID into it if node UUID not exists.
   *
   * @return node UUID
   */
  public static void generateNodeUUIDIfNotExist() throws IOException {
    File envFile = new File(ENV_FILE_PATH);
    if (envFile.createNewFile()) {
      // Do nothing, don't print log here
    }
    Properties envProperties = new Properties();
    try (FileReader reader = new FileReader(envFile)) {
      envProperties.load(reader);
    }
    if (envProperties.containsKey(NODE_UUID_IN_ENV_FILE)) {
      return;
    }
    final String uuid = String.valueOf(UUID.randomUUID());
    envProperties.put(NODE_UUID_IN_ENV_FILE, uuid);
    try (FileOutputStream fos = new FileOutputStream(envFile)) {
      envProperties.store(fos, null);
      fos.getFD().sync();
    }
  }

  public static String getNodeUUID() throws IOException, LicenseException {
    File envFile = new File(ENV_FILE_PATH);
    if (!envFile.exists()) {
      throw new LicenseException();
    }
    Properties envProperties = new Properties();
    try (FileReader reader = new FileReader(envFile)) {
      envProperties.load(reader);
    }
    if (!envProperties.containsKey(NODE_UUID_IN_ENV_FILE)) {
      throw new LicenseException();
    }
    return String.valueOf(envProperties.get(NODE_UUID_IN_ENV_FILE));
  }

  /**
   * This function should be called at the end of ConfigNode's launching. The existence of
   * system_info file will be treated as a signal, which means timecho-ConfigNode has started
   * successfully.
   */
  public static void generateSystemInfoFile() {
    try (FileOutputStream fos = new FileOutputStream(SYSTEM_INFO_FILE_PATH)) {
      String content = generateSystemInfoContentWithVersion();
      fos.write(content.getBytes());
      fos.getFD().sync();
      logger.info("{} file generated successfully. Content is {}", SYSTEM_INFO_FILE_PATH, content);
    } catch (Exception e) {
      logger.error("{} file generated fail.", SYSTEM_INFO_FILE_PATH);
    }
  }

  public static String generateSystemInfoContentWithVersion() throws LicenseException {
    return SYSTEM_INFO_VERSION + "-" + generateSystemInfoContentOfV02();
  }

  private static String systemInfoContentRemoveVersion(String contentWithVersion)
      throws LicenseException {
    if (contentWithVersion.charAt(VERSION_LENGTH) != '-') {
      throw new LicenseException("Cannot remove version from system info " + contentWithVersion);
    }
    return contentWithVersion.substring(VERSION_LENGTH + 1);
  }

  private static String generateSystemInfoContentOfV02() throws LicenseException {
    try {
      Properties hardwareInfoProperties = new SortedProperties();
      hardwareSystemInfoNameToItsGetter.forEach(
          (name, getter) -> hardwareInfoProperties.setProperty(name, getter.get()));
      Properties configurableInfoProperties = new SortedProperties();
      configurableSystemInfoNameToItsGetter.forEach(
          (name, getter) -> configurableInfoProperties.setProperty(name, getter.get()));
      Properties nodeUuidProperties = new SortedProperties();
      nodeUuidProperties.setProperty(License.NODE_UUID_NAME, getNodeUUID());
      List<Properties> propertiesList =
          Arrays.asList(hardwareInfoProperties, configurableInfoProperties, nodeUuidProperties);
      List<String> encodedList = new ArrayList<>();
      String combinedOriginal = "";
      for (Properties properties : propertiesList) {
        StringWriter stringWriter = new StringWriter();
        properties.store(stringWriter, null);
        String originalInfo = stringWriter.toString();
        combinedOriginal += originalInfo;
        // remove time
        if (originalInfo.charAt(0) == '#') {
          originalInfo = originalInfo.substring(originalInfo.indexOf('\n') + 1);
        }
        encodedList.add(encodeTo8(originalInfo));
      }
      try (FileOutputStream fos = new FileOutputStream(HISTORY_FILE_PATH, true)) {
        fos.write(RSA.publicEncrypt(combinedOriginal).getBytes());
        fos.write("\n".getBytes());
        fos.getFD().sync();
      }
      return encodedList.stream().reduce((a, b) -> a + "-" + b).get();
    } catch (Exception e) {
      throw new LicenseException(e);
    }
  }

  private static String generateSystemInfoContentOfV01() throws LicenseException {
    // create a Properties which has defined order during serializing
    Properties systemInfoProperties = new SortedProperties();
    try {
      // generate properties
      for (Entry<String, Supplier<String>> entry : systemInfoNameToItsGetter.entrySet()) {
        systemInfoProperties.setProperty(entry.getKey(), entry.getValue().get());
      }
      systemInfoProperties.setProperty(License.NODE_UUID_NAME, getNodeUUID());
      // store properties to file
      StringWriter stringWriter = new StringWriter();
      systemInfoProperties.store(stringWriter, null);
      String originSystemInfo = stringWriter.toString();
      // save original system info
      try (FileOutputStream fos = new FileOutputStream(HISTORY_FILE_PATH, true)) {
        fos.write(RSA.publicEncrypt(originSystemInfo).getBytes());
        fos.write("\n".getBytes());
        fos.getFD().sync();
      }
      // remove time comment
      if (originSystemInfo.charAt(0) == '#') {
        originSystemInfo = originSystemInfo.substring(originSystemInfo.indexOf('\n') + 1);
      }
      return systemInfoEncode(originSystemInfo);
    } catch (Exception e) {
      logger.error("generate system info content fail");
      throw new LicenseException(e);
    }
  }

  private static String systemInfoEncode(String originalInfo) throws NoSuchAlgorithmException {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    Base32 base32 = new Base32();
    String temp = base32.encodeAsString(md5.digest(originalInfo.getBytes())).substring(8, 24);
    return temp.substring(0, 8) + "-" + temp.substring(8, 16);
  }

  private static String encodeTo8(String originalInfo) throws NoSuchAlgorithmException {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    Base32 base32 = new Base32();
    return base32.encodeAsString(md5.digest(originalInfo.getBytes())).substring(0, 8);
  }

  // region verify system info

  public boolean verifyAllSystemInfo(Properties licenseProperties) throws LicenseException {
    return verifyAllSystemInfoOfEveryVersion(
        licenseProperties,
        hardwareSystemInfoNameToItsGetter,
        configurableSystemInfoNameToItsGetter);
  }

  public static boolean verifyAllSystemInfoOfEveryVersion(
      Properties licenseProperties,
      ImmutableMap<String, Supplier<String>> hardwareSystemInfoNameToItsGetter,
      ImmutableMap<String, Supplier<String>> configurableSystemInfoNameToItsGetter) {
    try {
      if (licenseProperties.containsKey(License.SYSTEM_INFO_HASH)) {
        String version =
            licenseProperties.getProperty(License.SYSTEM_INFO_HASH).substring(0, VERSION_LENGTH);
        if ("01".equals(version)) {
          return verifyAllSystemInfoOfV01(licenseProperties);
        } else if ("02".equals(version)) {
          return verifyAllSystemInfoOfV02(licenseProperties);
        } else {
          logger.error("License system info version {} is unsupported", version);
          return false;
        }
      } else {
        return verifyAllSystemInfoOfV00(
            licenseProperties,
            hardwareSystemInfoNameToItsGetter,
            configurableSystemInfoNameToItsGetter);
      }
    } catch (Exception ignored) {
      logger.error("Verify system info fail.");
      return false;
    }
  }

  //  public static boolean

  public static boolean verifyAllSystemInfoOfV02(Properties licenseProperties) throws Exception {
    String actualSystemInfo = generateSystemInfoContentOfV02();
    String licenseSystemInfo = licenseProperties.getProperty(License.SYSTEM_INFO_HASH);
    licenseSystemInfo = systemInfoContentRemoveVersion(licenseSystemInfo);
    if (Boolean.parseBoolean(
        licenseProperties.getProperty(License.SKIP_HARDWARE_SYSTEM_INFO_CHECK_NAME, null))) {
      // remove hardware hash code
      actualSystemInfo = actualSystemInfo.substring(9);
      licenseSystemInfo = licenseSystemInfo.substring(9);
    }
    return actualSystemInfo.equals(licenseSystemInfo);
  }

  public static boolean verifyAllSystemInfoOfV01(Properties licenseProperties) throws Exception {
    String actualSystemInfo = generateSystemInfoContentOfV01();
    String licenseSystemInfo = licenseProperties.getProperty(License.SYSTEM_INFO_HASH);
    return actualSystemInfo.equals(systemInfoContentRemoveVersion(licenseSystemInfo));
  }

  public static boolean verifyAllSystemInfoOfV00(
      Properties licenseProperties,
      ImmutableMap<String, Supplier<String>> hardwareSystemInfoNameToItsGetter,
      ImmutableMap<String, Supplier<String>> configurableSystemInfoNameToItsGetter)
      throws Exception {
    final boolean skipHardwareSystemInfoCheck =
        Boolean.parseBoolean(
            licenseProperties.getProperty(License.SKIP_HARDWARE_SYSTEM_INFO_CHECK_NAME, "false"));
    if (skipHardwareSystemInfoCheck) {
      logger.info(RSA.publicEncrypt("skip hardware system info check"));
    } else {
      for (Entry<String, Supplier<String>> entry : hardwareSystemInfoNameToItsGetter.entrySet()) {
        if (!verifySystemInfo(licenseProperties, entry.getKey(), entry.getValue().get(), true)) {
          return false;
        }
      }
    }
    for (Entry<String, Supplier<String>> entry : configurableSystemInfoNameToItsGetter.entrySet()) {
      if (!verifySystemInfo(licenseProperties, entry.getKey(), entry.getValue().get(), false)) {
        return false;
      }
    }
    if (licenseProperties.get(License.SYSTEM_UUID_NAME).toString().isEmpty()
        || skipHardwareSystemInfoCheck) {
      if (!verifySystemInfo(licenseProperties, License.NODE_UUID_NAME, getNodeUUID(), false)) {
        return false;
      }
    }
    return true;
  }

  private static boolean verifySystemInfo(
      Properties licenseProperties, String key, String systemActualValue, boolean allowEmpty)
      throws LicenseException {
    if (!licenseProperties.containsKey(key)) {
      String errorMessage = String.format("License does not contain the \"%s\" field.", key);
      logger.error(RSA.publicEncrypt(errorMessage));
      return false;
    }
    if (licenseProperties.get(key).equals("") && allowEmpty) {
      String warnMessage =
          String.format(
              "License's \"%s\" field is empty, and this field is allowed to be empty.", key);
      logger.warn(RSA.publicEncrypt(warnMessage));
      return true;
    }
    if (!licenseProperties.get(key).equals(systemActualValue)) {
      String licenseValue = String.valueOf(licenseProperties.get(key));
      String errorMessage =
          String.format(
              "License's \"%s\" field has value \"%s\", but system's actual value is \"%s\". To make this license work, these two parameters must be the same.",
              key, licenseValue, systemActualValue);
      logger.error(RSA.publicEncrypt(errorMessage));
      return false;
    }
    return true;
  }

  // endregion

  // region file operation

  @TestOnly
  public TSStatus setLicenseFile(String fileName, String content) {
    Path filePath = Paths.get(ACTIVATION_DIR_PATH, fileName);
    try (FileOutputStream fos = new FileOutputStream(filePath.toFile())) {
      fos.write(content.getBytes());
      fos.getFD().sync();
      logger.info("set license file success: {}", filePath);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (IOException e) {
      logger.error("set license file {} fail", filePath, e);
      TSStatus status = new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
      return status;
    }
  }

  @TestOnly
  public TSStatus deleteLicenseFile(String filename) {
    return deleteLicenseFile(filename, false);
  }

  @TestOnly
  public TSStatus deleteLicenseFile(String fileName, boolean allowFail) {
    Path filePath = Paths.get(ACTIVATION_DIR_PATH, fileName);
    if (!Files.exists(filePath)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    try {
      Files.delete(filePath);
    } catch (IOException e) {
      if (!allowFail) {
        logger.error("delete license file {} fail: {}", filePath, e);
        TSStatus status = new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode());
        status.setMessage(e.getMessage());
        return status;
      }
    }
    logger.info("delete license file：{}", filePath);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @TestOnly
  public TSStatus getLicenseFile(String fileName) {
    Path filePath = Paths.get(ACTIVATION_DIR_PATH, fileName);
    if (!Files.exists(filePath)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    try {
      byte[] buffer = Files.readAllBytes(filePath);
      TSStatus status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      status.message = new String(buffer);
      return status;
    } catch (IOException e) {
      logger.error("read license file {} fail: {}", filePath, e);
      TSStatus status = new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
      return status;
    }
  }

  // endregion

  // region security

  protected static String decryptV00(String src) throws LicenseException {
    return RSA.publicDecryptV00(src);
  }

  protected static String decryptV01(String src) throws LicenseException {
    // remove version
    String base32Raw = src.substring(3).replaceAll("-", "");
    return RSA.publicDecryptV01(base32Raw);
  }

  private void checkSystemTimeAndIssueTime() {
    if (!checkSystemTimeAndIssueTimeImpl(this.license)) {
      logger.error(
          "System time anomaly detected. Please check whether the system time is consistent with current actual time.");
      giveUpLicense("system time anomaly detected");
    }
  }

  static boolean checkSystemTimeAndIssueTimeImpl(License license) {
    long now = System.currentTimeMillis();
    return now >= license.getLicenseIssueTimestamp()
        || license.getLicenseIssueTimestamp() - now < ONE_DAY;
  }

  // endregion

  // region helpers
  @TestOnly
  public static Properties buildUnlimitedLicenseProperties() {
    Properties properties = new Properties();
    properties.setProperty(License.LICENSE_ISSUE_TIMESTAMP_NAME, "10000");
    properties.setProperty(License.LICENSE_EXPIRE_TIMESTAMP_NAME, "4102416000000");
    properties.setProperty(License.DATANODE_NUM_LIMIT_NAME, "9999");
    properties.setProperty(License.DATANODE_CPU_CORE_NUM_LIMIT_NAME, "999999");
    properties.setProperty(License.DEVICE_NUM_LIMIT_NAME, String.valueOf(Long.MAX_VALUE));
    properties.setProperty(License.SENSOR_NUM_LIMIT_NAME, String.valueOf(Long.MAX_VALUE));
    properties.setProperty(License.DISCONNECTION_FROM_ACTIVE_NODE_TIME_LIMIT_NAME, "10000");
    properties.setProperty(License.AINODE_NUM_LIMIT_NAME, "9999");
    return properties;
  }

  public void cliActivateCheckLicenseContentAvailable(List<String> encryptedLicenses)
      throws LicenseException, IOException {
    List<License> licenseList = new ArrayList<>();
    for (String encryptedLicense : encryptedLicenses) {
      Properties properties = loadLicenseFromEveryVersion(encryptedLicense);
      License license = new License(() -> {});
      license.loadFromProperties(properties, false);
      licenseList.add(license);
    }
    License firstLicense = licenseList.get(0);
    // check all consistent
    if (!licenseList.stream().allMatch(firstLicense::equals)) {
      throw new LicenseException(
          "Licenses' limitations are different, they must be consistent for CLI activation");
    }
    // check license enough for DataNodes num and CPU cores num
    if (firstLicense.getDataNodeNumLimit()
        < configManager.getNodeManager().getRegisteredDataNodeCount()) {
      throw new LicenseException(
          String.format(
              "License only allows %s DataNodes, but there are %s DataNodes in cluster",
              firstLicense.getDataNodeNumLimit(),
              configManager.getNodeManager().getRegisteredDataNodeCount()));
    }
    if (firstLicense.getDataNodeCpuCoreNumLimit()
        < configManager.getNodeManager().getDataNodeCpuCoreCount()) {
      throw new LicenseException(
          String.format(
              "License only allows %s CPU cores, but there are %s CPU cores in cluster",
              firstLicense.getDataNodeCpuCoreNumLimit(),
              configManager.getNodeManager().getDataNodeCpuCoreCount()));
    }
  }

  // endregion
}
