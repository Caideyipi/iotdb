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
package com.timecho.iotdb.os.conf;

import com.timecho.iotdb.os.conf.provider.AWSS3Config;
import com.timecho.iotdb.os.conf.provider.OSProviderConfig;
import com.timecho.iotdb.os.conf.provider.TestConfig;
import com.timecho.iotdb.os.utils.ObjectStorageType;

import java.io.File;

public class ObjectStorageConfig {
  private ObjectStorageType osType = ObjectStorageType.AWS_S3;

  private OSProviderConfig providerConfig = new AWSS3Config();

  private String[] cacheDirs = {
    "data" + File.separator + "datanode" + File.separator + "data" + File.separator + "cache"
  };

  private long cacheMaxDiskUsageInByte = 50 * 1024 * 1024 * 1024L;

  private int cachePageSizeInByte = 20 * 1024 * 1024;

  ObjectStorageConfig() {}

  public ObjectStorageType getOsType() {
    return osType;
  }

  public void setOsType(ObjectStorageType osType) {
    this.osType = osType;
    OSProviderConfig oldConfig = this.providerConfig;
    switch (osType) {
      case AWS_S3:
        this.providerConfig = new AWSS3Config();
        break;
      default:
        this.providerConfig = new TestConfig();
    }
    setEndpoint(oldConfig.getEndpoint());
    setBucketName(oldConfig.getBucketName());
    setAccessKeyId(oldConfig.getAccessKeyId());
    setAccessKeySecret(oldConfig.getAccessKeySecret());
  }

  public OSProviderConfig getProviderConfig() {
    return providerConfig;
  }

  public String getEndpoint() {
    return providerConfig.getEndpoint();
  }

  public void setEndpoint(String endpoint) {
    providerConfig.setEndpoint(endpoint);
  }

  public String getBucketName() {
    return providerConfig.getBucketName();
  }

  public void setBucketName(String bucketName) {
    providerConfig.setBucketName(bucketName);
  }

  public String getAccessKeyId() {
    return providerConfig.getAccessKeyId();
  }

  public void setAccessKeyId(String accessKeyId) {
    providerConfig.setAccessKeyId(accessKeyId);
  }

  public String getAccessKeySecret() {
    return providerConfig.getAccessKeySecret();
  }

  public void setAccessKeySecret(String accessKeySecret) {
    providerConfig.setAccessKeySecret(accessKeySecret);
  }

  public String[] getCacheDirs() {
    return cacheDirs;
  }

  public void setCacheDirs(String[] cacheDirs) {
    this.cacheDirs = cacheDirs;
  }

  public long getCacheMaxDiskUsageInByte() {
    return cacheMaxDiskUsageInByte;
  }

  public void setCacheMaxDiskUsageInByte(long cacheMaxDiskUsageInByte) {
    this.cacheMaxDiskUsageInByte = cacheMaxDiskUsageInByte;
  }

  public int getCachePageSizeInByte() {
    return cachePageSizeInByte;
  }

  public void setCachePageSizeInByte(int cachePageSizeInByte) {
    this.cachePageSizeInByte = cachePageSizeInByte;
  }
}
