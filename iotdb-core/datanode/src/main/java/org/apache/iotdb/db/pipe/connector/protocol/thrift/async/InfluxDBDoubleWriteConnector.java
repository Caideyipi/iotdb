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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.async;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeNonCriticalException;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.impl.InfluxDBImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_INFLUXDB_NODE_URLS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_INFLUXDB_NODE_URLS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_USER_KEY;

/** This connector is mainly used to detect receiver problems using influxDB as comparison. */
public class InfluxDBDoubleWriteConnector extends IoTDBDataRegionAsyncConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBDoubleWriteConnector.class);
  private InfluxDB influxDB;

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);
    influxDB =
        new InfluxDBImpl(
            parameters.getStringOrDefault(
                Arrays.asList(SINK_INFLUXDB_NODE_URLS_KEY, CONNECTOR_INFLUXDB_NODE_URLS_KEY),
                CONNECTOR_INFLUXDB_NODE_URLS_KEY),
            parameters.getStringOrDefault(
                Arrays.asList(SINK_IOTDB_USER_KEY, CONNECTOR_IOTDB_USER_KEY),
                CONNECTOR_IOTDB_USER_DEFAULT_VALUE),
            parameters.getStringOrDefault(
                Arrays.asList(SINK_IOTDB_PASSWORD_KEY, CONNECTOR_IOTDB_PASSWORD_KEY),
                CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE),
            new okhttp3.OkHttpClient.Builder());
    influxDB.createDatabase("root");
    influxDB.setDatabase("root");
  }

  @Override
  public void handshake() throws Exception {
    super.handshake();
    influxDB.ping();
  }

  @Override
  public void heartbeat() {
    super.heartbeat();
    influxDB.ping();
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    super.transfer(tabletInsertionEvent);
    doTransfer(tabletInsertionEvent);
  }

  private void doTransfer(TabletInsertionEvent tabletInsertionEvent) {
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "OpcUaConnector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
      transferTablet(((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).convertToTablet());
    } else {
      transferTablet(((PipeRawTabletInsertionEvent) tabletInsertionEvent).convertToTablet());
    }
  }

  private void transferTablet(Tablet tablet) {
    for (int columnIndex = 0; columnIndex < tablet.getSchemas().size(); ++columnIndex) {
      final TSDataType dataType = tablet.getSchemas().get(columnIndex).getType();
      for (int rowIndex = 0; rowIndex < tablet.rowSize; ++rowIndex) {
        // Filter null value
        if (tablet.bitMaps[columnIndex].isMarked(rowIndex)) {
          continue;
        }

        Point.Builder builder =
            Point.measurement(
                tablet.deviceId
                    + TsFileConstant.PATH_SEPARATOR
                    + tablet.getSchemas().get(columnIndex).getMeasurementId());

        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();

        builder.tag(tags);

        switch (dataType) {
          case BOOLEAN:
            fields.put("value", ((boolean[]) tablet.values[columnIndex])[rowIndex]);
            break;
          case INT32:
            fields.put("value", ((int[]) tablet.values[columnIndex])[rowIndex]);
            break;
          case INT64:
            fields.put("value", ((long[]) tablet.values[columnIndex])[rowIndex]);
            break;
          case FLOAT:
            fields.put("value", ((float[]) tablet.values[columnIndex])[rowIndex]);
            break;
          case DOUBLE:
            fields.put("value", ((double[]) tablet.values[columnIndex])[rowIndex]);
            break;
          case TEXT:
            fields.put("value", ((Binary[]) tablet.values[columnIndex])[rowIndex]);
            break;
          case VECTOR:
          case UNKNOWN:
          default:
            throw new PipeRuntimeNonCriticalException(
                "Unsupported data type: " + tablet.getSchemas().get(columnIndex).getType());
        }
        builder.fields(fields);
        builder.time(tablet.timestamps[rowIndex], TimeUnit.MILLISECONDS);
        Point point = builder.build();
        influxDB.write(point);
      }
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    super.transfer(tsFileInsertionEvent);
    try {
      for (final TabletInsertionEvent tabletInsertionEvent :
          tsFileInsertionEvent.toTabletInsertionEvents()) {
        doTransfer(tabletInsertionEvent);
      }
    } finally {
      tsFileInsertionEvent.close();
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    influxDB.close();
  }
}
