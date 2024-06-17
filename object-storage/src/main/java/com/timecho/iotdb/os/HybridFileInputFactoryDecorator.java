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
package com.timecho.iotdb.os;

import com.timecho.iotdb.os.conf.ObjectStorageConfig;
import com.timecho.iotdb.os.conf.ObjectStorageDescriptor;
import com.timecho.iotdb.os.io.ObjectStorageConnector;
import org.apache.tsfile.fileSystem.FSPath;
import org.apache.tsfile.fileSystem.fileInputFactory.FileInputFactory;
import org.apache.tsfile.fileSystem.fileInputFactory.HybridFileInputFactory;
import org.apache.tsfile.read.reader.TsFileInput;
import org.apache.tsfile.utils.FSUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HybridFileInputFactoryDecorator implements FileInputFactory {
  private static final Logger logger =
      LoggerFactory.getLogger(HybridFileInputFactoryDecorator.class);
  private static final ObjectStorageConfig config =
      ObjectStorageDescriptor.getInstance().getConfig();
  private static final FileInputFactory fileInputFactory = new HybridFileInputFactory();

  private static final Map<String, FSPath> localPath2RemotePath = new ConcurrentHashMap<>();

  private int dataNodeId;

  public HybridFileInputFactoryDecorator(int dataNodeId) {
    this.dataNodeId = dataNodeId;
  }

  @Override
  public TsFileInput getTsFileInput(String filePath) throws IOException {
    File file = new File(filePath);
    if (!file.exists() && ObjectStorageConnector.getConnector().isConnectorEnabled()) {
      return fileInputFactory.getTsFileInput(getTsFileRemotePath(file, dataNodeId).getPath());
    }
    return fileInputFactory.getTsFileInput(filePath);
  }

  public static void putRemotePathInfo(File localFile, FSPath remotePath) {
    try {
      localPath2RemotePath.put(localFile.getCanonicalPath(), remotePath);
    } catch (IOException e) {
      logger.warn("Fail to get canonical path of file {}", localFile);
      localPath2RemotePath.remove(localFile.getPath(), remotePath);
    }
  }

  public static FSPath removeRemotePathInfo(File localFile) {
    try {
      return localPath2RemotePath.remove(localFile.getCanonicalPath());
    } catch (IOException e) {
      logger.warn("Fail to get canonical path of file {}", localFile);
      return localPath2RemotePath.remove(localFile.getPath());
    }
  }

  public static FSPath getTsFileRemotePath(File localFile, int dataNodeId) throws IOException {
    String canonicalPath = localFile.getCanonicalPath();
    return localPath2RemotePath.getOrDefault(
        canonicalPath,
        FSUtils.parseLocalTsFile2OSFile(localFile, config.getBucketName(), dataNodeId));
  }
}
