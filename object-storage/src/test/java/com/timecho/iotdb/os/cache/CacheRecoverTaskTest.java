package com.timecho.iotdb.os.cache;

import com.timecho.iotdb.os.conf.ObjectStorageConfig;
import com.timecho.iotdb.os.conf.ObjectStorageDescriptor;
import com.timecho.iotdb.os.fileSystem.OSFile;
import com.timecho.iotdb.os.fileSystem.OSURI;
import com.timecho.iotdb.os.utils.ObjectStorageType;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class CacheRecoverTaskTest {
  private static final File cacheDir = new File("target" + File.separator + "cache");
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();

  private ObjectStorageType prevType;
  private int prevCachePageSize;
  private String[] prevCacheDirs;

  @Before
  public void setUp() throws Exception {
    prevType = config.getOsType();
    config.setOsType(ObjectStorageType.TEST);
    prevCachePageSize = config.getCachePageSizeInByte();
    config.setCachePageSizeInByte(100);
    prevCacheDirs = config.getCacheDirs();
    config.setCacheDirs(new String[] {cacheDir.getPath()});
    cacheDir.mkdirs();
    OSFileCache.getInstance().clear();
    CacheFileManager.getInstance().clear();
  }

  @After
  public void tearDown() throws Exception {
    config.setOsType(prevType);
    config.setCachePageSizeInByte(prevCachePageSize);
    config.setCacheDirs(prevCacheDirs);
    FileUtils.deleteDirectory(cacheDir);
    OSFileCache.getInstance().clear();
    CacheFileManager.getInstance().clear();
  }

  @Test
  public void testRecover() {
    OSFileCache cache = OSFileCache.getInstance();
    CacheFileManager cacheFileManager = CacheFileManager.getInstance();
    // prepare cache files
    OSFile osFile = new OSFile(new OSURI("test_bucket", "test_key"), ObjectStorageType.TEST);
    OSFileCacheKey key1 = new OSFileCacheKey(osFile, 0);
    byte[] data1 = new byte[100];
    Arrays.fill(data1, (byte) 1);
    cacheFileManager.persist(key1, data1);
    OSFileCacheKey key2 = new OSFileCacheKey(osFile, 100);
    byte[] data2 = new byte[100];
    Arrays.fill(data2, (byte) 2);
    cacheFileManager.persist(key2, data2);
    // reset
    long expectedId = cacheFileManager.getCacheFileId();
    cacheFileManager.setCacheFileId(0);
    long expectedSize = cacheFileManager.getTotalCacheFileSize();
    cacheFileManager.setTotalCacheFileSize(0);
    // recover
    assertNull(cache.getIfPresent(key1));
    assertNull(cache.getIfPresent(key2));
    new CacheRecoverTask().run();
    // check
    assertEquals(expectedId, cacheFileManager.getCacheFileId());
    assertEquals(expectedSize, cacheFileManager.getTotalCacheFileSize());
    assertNotNull(cache.getIfPresent(key1));
    assertNotNull(cache.getIfPresent(key2));
  }
}
