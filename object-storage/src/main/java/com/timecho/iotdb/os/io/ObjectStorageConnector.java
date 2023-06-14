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

package com.timecho.iotdb.os.io;

import com.timecho.iotdb.os.conf.ObjectStorageDescriptor;
import com.timecho.iotdb.os.exception.ObjectStorageException;
import com.timecho.iotdb.os.fileSystem.OSURI;
import com.timecho.iotdb.os.io.aws.S3ObjectStorageConnector;
import com.timecho.iotdb.os.io.test.TestObjectStorageConnector;
import com.timecho.iotdb.os.utils.ObjectStorageType;

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public interface ObjectStorageConnector {
  Map<ObjectStorageType, ObjectStorageConnector> cachedConnector = new ConcurrentHashMap<>();

  static ObjectStorageConnector getConnector(ObjectStorageType type) {
    switch (type) {
      case AWS_S3:
        return cachedConnector.computeIfAbsent(type, k -> new S3ObjectStorageConnector());
      case TEST:
        return cachedConnector.computeIfAbsent(type, k -> new TestObjectStorageConnector());
      default:
        return null;
    }
  }

  static ObjectStorageConnector getConnector() {
    return getConnector(ObjectStorageDescriptor.getInstance().getConfig().getOsType());
  }

  static void closeAll() {
    for (ObjectStorageConnector connector : cachedConnector.values()) {
      connector.close();
    }
  }

  boolean doesObjectExist(OSURI osUri) throws ObjectStorageException;

  IMetaData getMetaData(OSURI osUri) throws ObjectStorageException;

  boolean createNewEmptyObject(OSURI osUri) throws ObjectStorageException;

  boolean delete(OSURI osUri) throws ObjectStorageException;

  boolean renameTo(OSURI fromOSUri, OSURI toOSUri) throws ObjectStorageException;

  InputStream getInputStream(OSURI osUri) throws ObjectStorageException;

  OSURI[] list(OSURI osUri) throws ObjectStorageException;

  void putLocalFile(OSURI osUri, File lcoalFile) throws ObjectStorageException;

  byte[] getRemoteObject(OSURI osUri, long position, int len) throws ObjectStorageException;

  void copyObject(OSURI srcUri, OSURI destUri) throws ObjectStorageException;

  void deleteObjectsByPrefix(OSURI prefixUri) throws ObjectStorageException;

  void close();
}
