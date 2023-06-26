package com.timecho.iotdb.os.io.aws;

import com.timecho.iotdb.os.conf.ObjectStorageConfig;
import com.timecho.iotdb.os.conf.ObjectStorageDescriptor;
import com.timecho.iotdb.os.conf.provider.AWSS3Config;
import com.timecho.iotdb.os.fileSystem.OSURI;
import com.timecho.iotdb.os.utils.ObjectStorageType;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Before using this test class, you should fill in the s3 config in the setUp method. */
public class S3ObjectStorageConnectorTest {
  private static final File tmpDir = new File("target" + File.separator + "tmp");
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private S3ObjectStorageConnector connector;

  private ObjectStorageType prevType;

  @Before
  public void setUp() throws Exception {
    prevType = config.getOsType();
    config.setOsType(ObjectStorageType.AWS_S3);
    AWSS3Config s3Config = (AWSS3Config) config.getProviderConfig();
    s3Config.setEndpoint("cn-north-1");
    s3Config.setBucketName("");
    s3Config.setAccessKeyId("");
    s3Config.setAccessKeySecret("");
    connector = new S3ObjectStorageConnector();
    tmpDir.mkdirs();
  }

  @After
  public void tearDown() throws Exception {
    config.setOsType(prevType);
    FileUtils.deleteDirectory(tmpDir);
    connector.deleteObjectsByPrefix(new OSURI(config.getBucketName(), ""));
    connector.close();
  }

  @Ignore
  @Test
  public void testCreateAndDelete() throws Exception {
    OSURI s3uri = new OSURI(config.getBucketName(), "tmp.tsfile");
    assertFalse(connector.doesObjectExist(s3uri));
    connector.createNewEmptyObject(s3uri);
    assertTrue(connector.doesObjectExist(s3uri));
    connector.delete(s3uri);
    assertFalse(connector.doesObjectExist(s3uri));
  }

  @Ignore
  @Test
  public void testPutAndGetLocalFile() throws Exception {
    OSURI s3uri = new OSURI(config.getBucketName(), "tmp.tsfile");
    File localFile = new File(tmpDir, "tmp.tsfile");
    byte[] data = new byte[150];
    Arrays.fill(data, 0, 100, (byte) 1);
    Arrays.fill(data, 100, 150, (byte) 2);
    writeLocalFile(localFile, data);
    // put
    assertFalse(connector.doesObjectExist(s3uri));
    connector.putLocalFile(s3uri, localFile);
    assertTrue(connector.doesObjectExist(s3uri));
    assertEquals(localFile.length(), connector.getMetaData(s3uri).length());
    // get
    byte[] expectedData = new byte[100];
    Arrays.fill(expectedData, (byte) 1);
    byte[] actualData = connector.getRemoteObject(s3uri, 0, 100);
    assertArrayEquals(expectedData, actualData);
    expectedData = new byte[50];
    Arrays.fill(expectedData, (byte) 2);
    actualData = connector.getRemoteObject(s3uri, 100, 100);
    assertArrayEquals(expectedData, actualData);
    // stream
    try (InputStream in = connector.getInputStream(s3uri)) {
      actualData = new byte[150];
      int actualReadNum = in.read(actualData);
      assertEquals(150, actualReadNum);
      assertArrayEquals(data, actualData);
      actualReadNum = in.read(actualData);
      assertEquals(-1, actualReadNum);
    }
  }

  @Ignore
  @Test
  public void testRenameTo() throws Exception {
    OSURI file1 = new OSURI(config.getBucketName(), "tmp1.tsfile");
    OSURI file2 = new OSURI(config.getBucketName(), "tmp2.tsfile");

    assertFalse(connector.doesObjectExist(file1));
    assertFalse(connector.doesObjectExist(file2));

    connector.createNewEmptyObject(file1);
    assertTrue(connector.doesObjectExist(file1));
    assertFalse(connector.doesObjectExist(file2));

    connector.renameTo(file1, file2);
    assertFalse(connector.doesObjectExist(file1));
    assertTrue(connector.doesObjectExist(file2));
  }

  @Ignore
  @Test
  public void copyObject() throws Exception {
    OSURI file1 = new OSURI(config.getBucketName(), "tmp1.tsfile");
    OSURI file2 = new OSURI(config.getBucketName(), "tmp2.tsfile");

    assertFalse(connector.doesObjectExist(file1));
    assertFalse(connector.doesObjectExist(file2));

    connector.createNewEmptyObject(file1);
    assertTrue(connector.doesObjectExist(file1));
    assertFalse(connector.doesObjectExist(file2));

    connector.copyObject(file1, file2);
    assertTrue(connector.doesObjectExist(file1));
    assertTrue(connector.doesObjectExist(file2));
  }

  @Ignore
  @Test
  public void list() throws Exception {
    OSURI bucket = new OSURI(config.getBucketName(), "");
    OSURI[] objects = connector.list(bucket);
    assertEquals(0, objects.length);

    OSURI file1 = new OSURI(config.getBucketName(), "tmp.tsfile");
    connector.createNewEmptyObject(file1);
    OSURI file2 = new OSURI(config.getBucketName(), "0/tmp.tsfile");
    connector.createNewEmptyObject(file2);

    objects = connector.list(bucket);
    for (OSURI osuri : objects) {
      assertTrue(osuri.equals(file1) || osuri.equals(file2));
    }
  }

  @Ignore
  @Test
  public void deleteObjectsByPrefix() throws Exception {
    OSURI file1 = new OSURI(config.getBucketName(), "tmp.tsfile");
    connector.createNewEmptyObject(file1);
    OSURI file2 = new OSURI(config.getBucketName(), "0/tmp.tsfile");
    connector.createNewEmptyObject(file2);
    assertTrue(connector.doesObjectExist(file1));
    assertTrue(connector.doesObjectExist(file2));
    connector.deleteObjectsByPrefix(new OSURI(config.getBucketName(), ""));
    assertFalse(connector.doesObjectExist(file1));
    assertFalse(connector.doesObjectExist(file2));
  }

  private void writeLocalFile(File localFile, byte[] bytes) throws IOException {
    try (OutputStream out = Files.newOutputStream(localFile.toPath())) {
      out.write(bytes);
    }
  }
}
