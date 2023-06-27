package org.apache.iotdb.db.storageengine.dataregion.migration;

import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.generator.TsFileNameGenerator;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.FSFactory;
import org.apache.iotdb.tsfile.fileSystem.fsFactory.HybridFSFactory;

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
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "org.w3c.*"})
public class LocalMigrationTaskTest {
  private static final String MIGRATION_SOURCE_BASE_DIR =
      TestConstant.BASE_OUTPUT_PATH.concat("migration_src");
  private static final String MIGRATION_SOURCE_TIME_PARTITION_DIR =
      MIGRATION_SOURCE_BASE_DIR + File.separator + "sg" + File.separator + 0 + File.separator + 0;
  private static final String MIGRATION_DESTINATION_BASE_DIR =
      TestConstant.BASE_OUTPUT_PATH.concat("migration_destination");
  private static final String MIGRATION_DESTINATION_TIME_PARTITION_DIR =
      MIGRATION_DESTINATION_BASE_DIR
          + File.separator
          + "sg"
          + File.separator
          + 0
          + File.separator
          + 0;

  @Before
  public void setUp() throws Exception {
    new File(MIGRATION_SOURCE_TIME_PARTITION_DIR).mkdirs();
    new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR).mkdirs();
  }

  @After
  public void tearDown() throws Exception {
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
        MigrationTask.newTask(MigrationCause.TTL, tsfile, MIGRATION_DESTINATION_BASE_DIR);
    task.migrate();
    // check
    assertFalse(srcFile.exists());
    assertFalse(srcResourceFile.exists());
    assertFalse(srcModsFile.exists());
    assertTrue(destFile.exists());
    assertTrue(destResourceFile.exists());
    assertTrue(destModsFile.exists());
    assertEquals(1, tsfile.getTierLevel());
    assertEquals(destFile, tsfile.getTsFile());
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
        MigrationTask.newTask(MigrationCause.TTL, tsfile, MIGRATION_DESTINATION_BASE_DIR);
    task.migrate();
    // check
    assertFalse(srcFile.exists());
    assertFalse(srcResourceFile.exists());
    assertFalse(srcModsFile.exists());
    assertTrue(destFile.exists());
    assertTrue(destResourceFile.exists());
    assertFalse(destModsFile.exists());
    assertEquals(1, tsfile.getTierLevel());
    assertEquals(destFile, tsfile.getTsFile());
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
        MigrationTask.newTask(MigrationCause.TTL, tsfile, MIGRATION_DESTINATION_BASE_DIR);
    task.migrate();
    // check
    assertFalse(srcFile.exists());
    assertFalse(srcResourceFile.exists());
    assertFalse(srcModsFile.exists());
    assertTrue(destFile.exists());
    assertTrue(destResourceFile.exists());
    assertFalse(destModsFile.exists());
    assertEquals(1, tsfile.getTierLevel());
    assertEquals(destFile, tsfile.getTsFile());
  }

  @Test
  @PrepareForTest(HybridFSFactory.class)
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
    srcModsFile.createNewFile();
    File destFile = new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName);
    File destResourceFile =
        new File(
            MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + TsFileResource.RESOURCE_SUFFIX);
    File destModsFile =
        new File(MIGRATION_DESTINATION_TIME_PARTITION_DIR, fileName + ModificationFile.FILE_SUFFIX);
    // mock
    TsFileResource tsfile = new TsFileResource(srcFile);
    FSFactory prevFSFactory = FSFactoryProducer.getFSFactory();
    try {
      HybridFSFactory fsFactory = PowerMockito.spy(new HybridFSFactory());
      PowerMockito.doThrow(new IOException())
          .when(fsFactory)
          .copyFile(Mockito.any(), Mockito.any());
      FSFactoryProducer.setFsFactory(fsFactory);
      // migrate
      MigrationTask task =
          MigrationTask.newTask(MigrationCause.TTL, tsfile, MIGRATION_DESTINATION_BASE_DIR);
      task.migrate();
    } finally {
      FSFactoryProducer.setFsFactory(prevFSFactory);
    }
    // check
    assertTrue(srcFile.exists());
    assertTrue(srcResourceFile.exists());
    assertTrue(srcModsFile.exists());
    assertFalse(destFile.exists());
    assertFalse(destResourceFile.exists());
    assertFalse(destModsFile.exists());
    assertEquals(0, tsfile.getTierLevel());
    assertEquals(srcFile, tsfile.getTsFile());
  }
}
