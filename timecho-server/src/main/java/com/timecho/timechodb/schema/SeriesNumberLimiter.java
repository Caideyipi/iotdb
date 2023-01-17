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
package com.timecho.timechodb.schema;

import org.apache.iotdb.external.api.ISeriesNumerMonitor;

import com.timecho.timechodb.license.LicenseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SeriesNumberLimiter implements ISeriesNumerMonitor {
  private static final Logger logger = LoggerFactory.getLogger(SeriesNumberLimiter.class);

  private long maxAllowedSeriesNumber = 10000000;

  private long currentSeriesNumber = 0;

  @Override
  public void init(Properties properties) {
    logger.info("series number init successfully");
    long maxAllowedTimeSeriesNumber = LicenseManager.getInstance().getMaxAllowedTimeSeriesNumber();
    this.maxAllowedSeriesNumber =
        maxAllowedTimeSeriesNumber != 0 ? maxAllowedTimeSeriesNumber : maxAllowedSeriesNumber;
    currentSeriesNumber = 0;
  }

  @Override
  public boolean addTimeSeries(int number) {
    if (logger.isDebugEnabled()) {
      logger.info(
          "max allowed series number:{},curr exist series number:{}",
          maxAllowedSeriesNumber,
          currentSeriesNumber);
    }

    if (currentSeriesNumber + number > maxAllowedSeriesNumber) {
      return false;
    } else {
      currentSeriesNumber += number;
      return true;
    }
  }

  @Override
  public void deleteTimeSeries(int number) {
    currentSeriesNumber -= number;
  }

  public static SeriesNumberLimiter getInstance() {
    return SeriesNumberLimiterHolder.INSTANCE;
  }

  private static class SeriesNumberLimiterHolder {
    private static final SeriesNumberLimiter INSTANCE = new SeriesNumberLimiter();
  }
}
