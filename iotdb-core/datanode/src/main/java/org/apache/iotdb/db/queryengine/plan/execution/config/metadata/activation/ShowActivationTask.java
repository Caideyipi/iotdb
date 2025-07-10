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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.activation;

import org.apache.iotdb.common.rpc.thrift.TLicense;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TClusterActivationStatus;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ShowActivationTask implements IConfigTask {

  private static String UNLIMITED = "Unlimited";

  private static DateTimeFormatter dateFormat =
      DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showActivation();
  }

  public static void buildTsBlock(
      TLicense license,
      TLicense usage,
      TClusterActivationStatus clusterActivationStatus,
      SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showActivationColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    Instant instant = Instant.ofEpochMilli(license.getExpireTimestamp());
    ZonedDateTime zonedDateTime =
        instant.atZone(SessionManager.getInstance().getSessionTimeZone().toZoneId());
    String formattedTime = dateFormat.format(zonedDateTime);
    List<List<String>> table =
        Arrays.asList(
            Arrays.asList("Status", clusterActivationStatus.toString(), "-"),
            Arrays.asList("ExpiredTime", "-", formattedTime),
            Arrays.asList(
                "DataNodeLimit",
                format(usage.getDataNodeNum()),
                unlimitedOrFormat(license.getDataNodeNum())),
            Arrays.asList(
                "AiNodeLimit",
                format(usage.getAiNodeNum()),
                unlimitedOrFormat(license.getAiNodeNum())),
            Arrays.asList(
                "CpuLimit",
                format(usage.getCpuCoreNum()),
                unlimitedOrFormat(license.getCpuCoreNum())),
            Arrays.asList(
                "DeviceLimit",
                format(usage.getDeviceNum()),
                unlimitedOrFormat(license.getDeviceNum())),
            Arrays.asList(
                "TimeSeriesLimit",
                format(usage.getSensorNum()),
                unlimitedOrFormat(license.getSensorNum())));
    for (List<String> row : table) {
      int col = 0;
      builder.getTimeColumnBuilder().writeLong(0);
      for (String unit : row) {
        builder.getColumnBuilder(col).writeBinary(new Binary(unit, TSFileConfig.STRING_CHARSET));
        col++;
      }
      builder.declarePosition();
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowActivationHeader();
    ConfigTaskResult result =
        new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader);
    future.set(result);
  }

  private static String format(long x) {
    return DecimalFormat.getNumberInstance().format(x);
  }

  private static String unlimitedOrFormat(short x) {
    if (x == Short.MAX_VALUE) {
      return UNLIMITED;
    }
    return format(x);
  }

  private static String unlimitedOrFormat(int x) {
    if (x == Integer.MAX_VALUE) {
      return UNLIMITED;
    }
    return format(x);
  }

  private static String unlimitedOrFormat(long x) {
    if (x == Long.MAX_VALUE) {
      return UNLIMITED;
    }
    return format(x);
  }
}
