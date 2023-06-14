package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.constant.TestConstant;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileNameGenerator;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.fileSystem.FSType;
import org.apache.iotdb.tsfile.utils.FSUtils;

import com.timecho.iotdb.os.conf.ObjectStorageConfig;
import com.timecho.iotdb.os.conf.ObjectStorageDescriptor;
import com.timecho.iotdb.os.conf.provider.TestConfig;
import com.timecho.iotdb.os.exception.ObjectStorageException;
import com.timecho.iotdb.os.io.ObjectStorageConnector;
import com.timecho.iotdb.os.utils.ObjectStorageType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "org.w3c.*"})
public class RemoteMigrationTaskTest {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final TSFileConfig tsfileConfig = TSFileDescriptor.getInstance().getConfig();
  private static final ObjectStorageConfig osConfig =
      ObjectStorageDescriptor.getInstance().getConfig();
  private static final String MIGRATION_SOURCE_BASE_DIR =
      TestConstant.BASE_OUTPUT_PATH.concat("migration_src");
  private static final String MIGRATION_SOURCE_TIME_PARTITION_DIR =
      MIGRATION_SOURCE_BASE_DIR + File.separator + "sg" + File.separator + 0 + File.separator + 0;
  private static final String MIGRATION_DESTINATION_OS_DIR =
      FSUtils.getOSDefaultPath("migration_destination", config.getDataNodeId());
  private static final String MIGRATION_DESTINATION_BASE_DIR =
      TestConstant.BASE_OUTPUT_PATH.concat("migration_destination");
  private static final String MIGRATION_DESTINATION_TIME_PARTITION_DIR =
      MIGRATION_DESTINATION_BASE_DIR
          + File.separator
          + config.getDataNodeId()
          + File.separator
          + "sg"
          + File.separator
          + 0
          + File.separator
          + 0;
  private FSType[] prevTSFileStorageFs;
  private ObjectStorageType prevOSType;

  @Before
  public void setUp() throws Exception {
    prevTSFileStorageFs = tsfileConfig.getTSFileStorageFs();
    tsfileConfig.setTSFileStorageFs(new FSType[] {FSType.LOCAL, FSType.OBJECT_STORAGE});
    FSUtils.reload();
    prevOSType = osConfig.getOsType();
    osConfig.setOsType(ObjectStorageType.TEST);
    ((TestConfig) osConfig.getProviderConfig()).setTestDir(MIGRATION_DESTINATION_BASE_DIR);
    new File(MIGRATION_SOURCE_TIME_PARTITION_DIR).mkdirs();
    new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR).mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    tsfileConfig.setTSFileStorageFs(prevTSFileStorageFs);
    FSUtils.reload();
    osConfig.setOsType(prevOSType);
    EnvironmentUtils.cleanDir(MIGRATION_SOURCE_BASE_DIR);
    EnvironmentUtils.cleanDir(MIGRATION_DESTINATION_BASE_DIR);
  }

  @Test
  public void testNormalMigrateWithMods() throws Exception {
    String fileName = TsFileNameGenerator.generateNewTsFileName(0, 0, 0, 0);
    // create source files
    File srcFile = new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName);
    srcFile.createNewFile();
    File srcResourceFile =
        new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName + TsFileResource.RESOURCE_SUFFIX);
    srcResourceFile.createNewFile();
    File srcModsFile =
        new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName + ModificationFile.FILE_SUFFIX);
    srcModsFile.createNewFile();
    File destFile = new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName);
    File destResourceFile =
        new File(
            MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + TsFileResource.RESOURCE_SUFFIX);
    File destModsFile =
        new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + ModificationFile.FILE_SUFFIX);
    // migrate
    TsFileResource tsfile = new TsFileResource(srcFile);
    MigrationTask task =
        MigrationTask.newTask(MigrationCause.TTL, tsfile, MIGRATION_DESTINATION_OS_DIR);
    task.migrate();
    // check
    assertFalse(srcFile.exists());
    assertTrue(srcResourceFile.exists());
    assertTrue(srcModsFile.exists());
    assertTrue(destFile.exists());
    assertTrue(destResourceFile.exists());
    assertFalse(destModsFile.exists());
    assertEquals(1, tsfile.getTierLevel());
    assertEquals(srcFile, tsfile.getTsFile());
  }

  @Test
  public void testNormalMigrateWithoutMods() throws Exception {
    String fileName = TsFileNameGenerator.generateNewTsFileName(0, 0, 0, 0);
    // create source files
    File srcFile = new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName);
    srcFile.createNewFile();
    File srcResourceFile =
        new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName + TsFileResource.RESOURCE_SUFFIX);
    srcResourceFile.createNewFile();
    File srcModsFile =
        new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName + ModificationFile.FILE_SUFFIX);
    File destFile = new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName);
    File destResourceFile =
        new File(
            MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + TsFileResource.RESOURCE_SUFFIX);
    File destModsFile =
        new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + ModificationFile.FILE_SUFFIX);
    // migrate
    TsFileResource tsfile = new TsFileResource(srcFile);
    MigrationTask task =
        MigrationTask.newTask(MigrationCause.TTL, tsfile, MIGRATION_DESTINATION_OS_DIR);
    task.migrate();
    // check
    assertFalse(srcFile.exists());
    assertTrue(srcResourceFile.exists());
    assertFalse(srcModsFile.exists());
    assertTrue(destFile.exists());
    assertTrue(destResourceFile.exists());
    assertFalse(destModsFile.exists());
    assertEquals(1, tsfile.getTierLevel());
    assertEquals(srcFile, tsfile.getTsFile());
  }

  @Test
  public void testMigrateWithLeftFiles() throws Exception {
    String fileName = TsFileNameGenerator.generateNewTsFileName(0, 0, 0, 0);
    // create source files
    File srcFile = new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName);
    srcFile.createNewFile();
    File srcResourceFile =
        new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName + TsFileResource.RESOURCE_SUFFIX);
    srcResourceFile.createNewFile();
    File srcModsFile =
        new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName + ModificationFile.FILE_SUFFIX);
    File destFile = new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName);
    destFile.createNewFile();
    File destResourceFile =
        new File(
            MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + TsFileResource.RESOURCE_SUFFIX);
    destResourceFile.createNewFile();
    File destModsFile =
        new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + ModificationFile.FILE_SUFFIX);
    destModsFile.createNewFile();
    // migrate
    TsFileResource tsfile = new TsFileResource(srcFile);
    MigrationTask task =
        MigrationTask.newTask(MigrationCause.TTL, tsfile, MIGRATION_DESTINATION_OS_DIR);
    task.migrate();
    // check
    assertFalse(srcFile.exists());
    assertTrue(srcResourceFile.exists());
    assertFalse(srcModsFile.exists());
    assertTrue(destFile.exists());
    assertTrue(destResourceFile.exists());
    assertFalse(destModsFile.exists());
    assertEquals(1, tsfile.getTierLevel());
    assertEquals(srcFile, tsfile.getTsFile());
  }

  @Test
  @PrepareForTest(ObjectStorageConnector.class)
  public void testMigrateWithCopyErrors() throws Exception {
    String fileName = TsFileNameGenerator.generateNewTsFileName(0, 0, 0, 0);
    // create source files
    File srcFile = new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName);
    srcFile.createNewFile();
    File srcResourceFile =
        new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName + TsFileResource.RESOURCE_SUFFIX);
    srcResourceFile.createNewFile();
    File srcModsFile =
        new File(MIGRATION_SOURCE_TIME_PARTITION_DIR, fileName + ModificationFile.FILE_SUFFIX);
    File destFile = new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName);
    File destResourceFile =
        new File(
            MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + TsFileResource.RESOURCE_SUFFIX);
    File destModsFile =
        new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + ModificationFile.FILE_SUFFIX);
    // mock
    ObjectStorageConnector mockConnector = PowerMockito.mock(ObjectStorageConnector.class);
    PowerMockito.doThrow(new ObjectStorageException())
        .when(mockConnector)
        .putLocalFile(Mockito.any(), Mockito.any());
    PowerMockito.mockStatic(ObjectStorageConnector.class);
    PowerMockito.when(ObjectStorageConnector.getConnector(ObjectStorageType.TEST))
        .thenReturn(mockConnector);
    // migrate
    TsFileResource tsfile = new TsFileResource(srcFile);
    MigrationTask task =
        MigrationTask.newTask(MigrationCause.TTL, tsfile, MIGRATION_DESTINATION_OS_DIR);
    task.migrate();
    // check
    assertTrue(srcFile.exists());
    assertTrue(srcResourceFile.exists());
    assertFalse(srcModsFile.exists());
    assertFalse(destFile.exists());
    assertFalse(destResourceFile.exists());
    assertFalse(destModsFile.exists());
    assertEquals(0, tsfile.getTierLevel());
    assertEquals(srcFile, tsfile.getTsFile());
  }
}
