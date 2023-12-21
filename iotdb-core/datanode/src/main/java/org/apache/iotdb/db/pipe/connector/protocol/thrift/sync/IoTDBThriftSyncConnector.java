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

package org.apache.iotdb.db.pipe.connector.protocol.thrift.sync;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncConnectorClient;
import org.apache.iotdb.commons.pipe.plugin.builtin.connector.iotdb.IoTDBConnector;
import org.apache.iotdb.commons.pipe.connector.payload.response.PipeTransferFilePieceResp;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.builder.IoTDBThriftSyncPipeTransferBatchReqBuilder;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBatchReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.pipe.connector.protocol.IoTDBConnector;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeRawTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.storageengine.dataregion.wal.exception.WALPipeException;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferResp;

import com.google.common.collect.ImmutableList;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.CONNECTOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeConnectorConstant.SINK_KEY;
import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.IOTDB_THRIFT_CONNECTOR;
import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.IOTDB_THRIFT_SSL_CONNECTOR;
import static org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin.IOTDB_THRIFT_SSL_SINK;

public class IoTDBThriftSyncConnector extends IoTDBConnector {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBThriftSyncConnector.class);

  private static final PipeConfig PIPE_CONFIG = PipeConfig.getInstance();

  private final List<IoTDBThriftSyncConnectorClient> clients = new ArrayList<>();
  private final List<Boolean> isClientAlive = new ArrayList<>();

  private boolean useSSL;
  private String trustStorePath;
  private String trustStorePwd;

  private long currentClientIndex = 0;

  private IoTDBThriftSyncPipeTransferBatchReqBuilder tabletBatchBuilder;

  public IoTDBThriftSyncConnector() {
    // Do nothing
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final IoTDBConfig ioTDBConfig = IoTDBDescriptor.getInstance().getConfig();
    final PipeParameters parameters = validator.getParameters();

    final String userSpecifiedConnectorName =
        parameters
            .getStringOrDefault(
                ImmutableList.of(CONNECTOR_KEY, SINK_KEY),
                IOTDB_THRIFT_CONNECTOR.getPipePluginName())
            .toLowerCase();
    final Set<TEndPoint> givenNodeUrls = parseNodeUrls(parameters);

    validator
        .validate(
            empty -> {
              try {
                // Ensure the sink doesn't point to the thrift receiver on DataNode itself
                return !NodeUrlUtils.containsLocalAddress(
                    givenNodeUrls.stream()
                        .filter(tEndPoint -> tEndPoint.getPort() == ioTDBConfig.getRpcPort())
                        .map(TEndPoint::getIp)
                        .collect(Collectors.toList()));
              } catch (UnknownHostException e) {
                LOGGER.warn("Unknown host when checking pipe sink IP.", e);
                return false;
              }
            },
            String.format(
                "One of the endpoints %s of the receivers is pointing back to the thrift receiver %s on sender itself, or unknown host when checking pipe sink IP.",
                givenNodeUrls,
                new TEndPoint(ioTDBConfig.getRpcAddress(), ioTDBConfig.getRpcPort())))
        .validate(
            args -> !((boolean) args[0]) || ((boolean) args[1] && (boolean) args[2]),
            String.format(
                "When ssl transport is enabled, %s and %s must be specified",
                SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY, SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY),
            IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName().equals(userSpecifiedConnectorName)
                || IOTDB_THRIFT_SSL_SINK.getPipePluginName().equals(userSpecifiedConnectorName)
                || parameters.getBooleanOrDefault(SINK_IOTDB_SSL_ENABLE_KEY, false),
            parameters.hasAttribute(SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY),
            parameters.hasAttribute(SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY));
  }

  @Override
  public void customize(PipeParameters parameters, PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    super.customize(parameters, configuration);

    for (int i = 0; i < nodeUrls.size(); i++) {
      isClientAlive.add(false);
      clients.add(null);
    }

    if (isTabletBatchModeEnabled) {
      tabletBatchBuilder = new IoTDBThriftSyncPipeTransferBatchReqBuilder(parameters);
    }

    final String userSpecifiedConnectorName =
        parameters
            .getStringOrDefault(
                ImmutableList.of(CONNECTOR_KEY, SINK_KEY),
                IOTDB_THRIFT_CONNECTOR.getPipePluginName())
            .toLowerCase();
    useSSL =
        IOTDB_THRIFT_SSL_CONNECTOR.getPipePluginName().equals(userSpecifiedConnectorName)
            || IOTDB_THRIFT_SSL_SINK.getPipePluginName().equals(userSpecifiedConnectorName)
            || parameters.getBooleanOrDefault(SINK_IOTDB_SSL_ENABLE_KEY, false);
    trustStorePath = parameters.getString(SINK_IOTDB_SSL_TRUST_STORE_PATH_KEY);
    trustStorePwd = parameters.getString(SINK_IOTDB_SSL_TRUST_STORE_PWD_KEY);
  }

  @Override
  public void handshake() throws Exception {
    for (int i = 0; i < clients.size(); i++) {
      if (Boolean.TRUE.equals(isClientAlive.get(i))) {
        continue;
      }

      final String ip = nodeUrls.get(i).getIp();
      final int port = nodeUrls.get(i).getPort();

      // Close the client if necessary
      if (clients.get(i) != null) {
        try {
          clients.set(i, null).close();
        } catch (Exception e) {
          LOGGER.warn(
              "Failed to close client with target server ip: {}, port: {}, because: {}. Ignore it.",
              ip,
              port,
              e.getMessage());
        }
      }

      clients.set(
          i,
          new IoTDBThriftSyncConnectorClient(
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs((int) PIPE_CONFIG.getPipeConnectorHandshakeTimeoutMs())
                  .setRpcThriftCompressionEnabled(
                      PIPE_CONFIG.isPipeConnectorRPCThriftCompressionEnabled())
                  .build(),
              ip,
              port,
              useSSL,
              trustStorePath,
              trustStorePwd));
      try {
        final TPipeTransferResp resp =
            clients
                .get(i)
                .pipeTransfer(
                    PipeTransferDataNodeHandshakeReq.toTPipeTransferReq(
                        CommonDescriptor.getInstance().getConfig().getTimestampPrecision()));
        if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.warn(
              "Handshake error with target server ip: {}, port: {}, because: {}.",
              ip,
              port,
              resp.status);
        } else {
          isClientAlive.set(i, true);
          clients
              .get(i)
              .setTimeout((int) PipeConfig.getInstance().getPipeConnectorTransferTimeoutMs());
          LOGGER.info("Handshake success. Target server ip: {}, port: {}", ip, port);
        }
      } catch (TException e) {
        LOGGER.warn(
            "Handshake error with target server ip: {}, port: {}, because: {}.",
            ip,
            port,
            e.getMessage());
      }
    }

    for (int i = 0; i < clients.size(); i++) {
      if (Boolean.TRUE.equals(isClientAlive.get(i))) {
        return;
      }
    }
    throw new PipeConnectionException(
        String.format("All target servers %s are not available.", nodeUrls));
  }

  @Override
  public void heartbeat() {
    try {
      handshake();
    } catch (Exception e) {
      LOGGER.warn(
          "Failed to reconnect to target server, because: {}. Try to reconnect later.",
          e.getMessage(),
          e);
    }
  }

  @Override
  public void transfer(TabletInsertionEvent tabletInsertionEvent) throws Exception {
    // PipeProcessor can change the type of TabletInsertionEvent
    if (!(tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent)
        && !(tabletInsertionEvent instanceof PipeRawTabletInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftSyncConnector only support "
              + "PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. "
              + "Ignore {}.",
          tabletInsertionEvent);
      return;
    }

    if (((EnrichedEvent) tabletInsertionEvent).shouldParsePatternOrTime()) {
      if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
        transfer(
            ((PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent).parseEventWithPattern());
      } else { // tabletInsertionEvent instanceof PipeRawTabletInsertionEvent
        transfer(((PipeRawTabletInsertionEvent) tabletInsertionEvent).parseEventWithPattern());
      }
      return;
    }

    final int clientIndex = nextClientIndex();
    final IoTDBThriftSyncConnectorClient client = clients.get(clientIndex);

    try {
      if (isTabletBatchModeEnabled) {
        if (tabletBatchBuilder.onEvent(tabletInsertionEvent)) {
          doTransfer(client);
        }
      } else {
        if (tabletInsertionEvent instanceof PipeInsertNodeTabletInsertionEvent) {
          doTransfer(client, (PipeInsertNodeTabletInsertionEvent) tabletInsertionEvent);
        } else {
          doTransfer(client, (PipeRawTabletInsertionEvent) tabletInsertionEvent);
        }
      }
    } catch (TException e) {
      isClientAlive.set(clientIndex, false);

      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tablet insertion event %s, because %s.",
              tabletInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(TsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    // PipeProcessor can change the type of tsFileInsertionEvent
    if (!(tsFileInsertionEvent instanceof PipeTsFileInsertionEvent)) {
      LOGGER.warn(
          "IoTDBThriftSyncConnector only support PipeTsFileInsertionEvent. Ignore {}.",
          tsFileInsertionEvent);
      return;
    }

    if (!((PipeTsFileInsertionEvent) tsFileInsertionEvent).waitForTsFileClose()) {
      LOGGER.warn(
          "Pipe skipping temporary TsFile which shouldn't be transferred: {}",
          ((PipeTsFileInsertionEvent) tsFileInsertionEvent).getTsFile());
      return;
    }

    if (((EnrichedEvent) tsFileInsertionEvent).shouldParsePatternOrTime()) {
      for (final TabletInsertionEvent event : tsFileInsertionEvent.toTabletInsertionEvents()) {
        transfer(event);
      }
      return;
    }

    final int clientIndex = nextClientIndex();
    final IoTDBThriftSyncConnectorClient client = clients.get(clientIndex);

    try {
      // in order to commit in order
      if (isTabletBatchModeEnabled && !tabletBatchBuilder.isEmpty()) {
        doTransfer(client);
      }

      doTransfer(client, (PipeTsFileInsertionEvent) tsFileInsertionEvent);
    } catch (TException e) {
      isClientAlive.set(clientIndex, false);

      throw new PipeConnectionException(
          String.format(
              "Network error when transfer tsfile insertion event %s, because %s.",
              tsFileInsertionEvent, e.getMessage()),
          e);
    }
  }

  @Override
  public void transfer(Event event) throws TException, IOException {
    // in order to commit in order
    if (isTabletBatchModeEnabled && !tabletBatchBuilder.isEmpty()) {
      doTransfer(clients.get(nextClientIndex()));
    }

    if (!(event instanceof PipeHeartbeatEvent)) {
      LOGGER.warn(
          "IoTDBThriftSyncConnector does not support transferring generic event: {}.", event);
    }
  }

  private void doTransfer(IoTDBThriftSyncConnectorClient client) throws IOException, TException {
    final TPipeTransferResp resp =
        client.pipeTransfer(
            PipeTransferTabletBatchReq.toTPipeTransferReq(
                tabletBatchBuilder.getTPipeTransferReqs()));

    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer PipeTransferTabletBatchReq error, result status %s", resp.status));
    }

    tabletBatchBuilder.onSuccess();
  }

  private void doTransfer(
      IoTDBThriftSyncConnectorClient client,
      PipeInsertNodeTabletInsertionEvent pipeInsertNodeTabletInsertionEvent)
      throws PipeException, TException, WALPipeException {
    final TPipeTransferResp resp =
        pipeInsertNodeTabletInsertionEvent.getInsertNodeViaCacheIfPossible() == null
            ? client.pipeTransfer(
                PipeTransferTabletBinaryReq.toTPipeTransferReq(
                    pipeInsertNodeTabletInsertionEvent.getByteBuffer()))
            : client.pipeTransfer(
                PipeTransferTabletInsertNodeReq.toTPipeTransferReq(
                    pipeInsertNodeTabletInsertionEvent.getInsertNode()));

    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer PipeInsertNodeTabletInsertionEvent %s error, result status %s",
              pipeInsertNodeTabletInsertionEvent, resp.status));
    }
  }

  private void doTransfer(
      IoTDBThriftSyncConnectorClient client,
      PipeRawTabletInsertionEvent pipeRawTabletInsertionEvent)
      throws PipeException, TException, IOException {
    final TPipeTransferResp resp =
        client.pipeTransfer(
            PipeTransferTabletRawReq.toTPipeTransferReq(
                pipeRawTabletInsertionEvent.convertToTablet(),
                pipeRawTabletInsertionEvent.isAligned()));

    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format(
              "Transfer PipeRawTabletInsertionEvent %s error, result status %s",
              pipeRawTabletInsertionEvent, resp.status));
    }
  }

  private void doTransfer(
      IoTDBThriftSyncConnectorClient client, PipeTsFileInsertionEvent pipeTsFileInsertionEvent)
      throws PipeException, TException, IOException {
    final File tsFile = pipeTsFileInsertionEvent.getTsFile();

    // 1. Transfer file piece by piece
    final int readFileBufferSize = PipeConfig.getInstance().getPipeConnectorReadFileBufferSize();
    final byte[] readBuffer = new byte[readFileBufferSize];
    long position = 0;
    try (final RandomAccessFile reader = new RandomAccessFile(tsFile, "r")) {
      while (true) {
        final int readLength = reader.read(readBuffer);
        if (readLength == -1) {
          break;
        }

        final PipeTransferFilePieceResp resp =
            PipeTransferFilePieceResp.fromTPipeTransferResp(
                client.pipeTransfer(
                    PipeTransferTsFilePieceReq.toTPipeTransferReq(
                        tsFile.getName(),
                        position,
                        readLength == readFileBufferSize
                            ? readBuffer
                            : Arrays.copyOfRange(readBuffer, 0, readLength))));
        position += readLength;

        // This case only happens when the connection is broken, and the connector is reconnected
        // to the receiver, then the receiver will redirect the file position to the last position
        if (resp.getStatus().getCode()
            == TSStatusCode.PIPE_TRANSFER_FILE_OFFSET_RESET.getStatusCode()) {
          position = resp.getEndWritingOffset();
          reader.seek(position);
          LOGGER.info("Redirect file position to {}.", position);
          continue;
        }

        if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new PipeException(
              String.format("Transfer file %s error, result status %s.", tsFile, resp.getStatus()));
        }
      }
    }

    // 2. Transfer file seal signal, which means the file is transferred completely
    final TPipeTransferResp resp =
        client.pipeTransfer(
            PipeTransferTsFileSealReq.toTPipeTransferReq(tsFile.getName(), tsFile.length()));
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new PipeException(
          String.format("Seal file %s error, result status %s.", tsFile, resp.getStatus()));
    } else {
      LOGGER.info("Successfully transferred file {}.", tsFile);
    }
  }

  private int nextClientIndex() {
    final int clientSize = clients.size();
    // Round-robin, find the next alive client
    for (int tryCount = 0; tryCount < clientSize; ++tryCount) {
      final int clientIndex = (int) (currentClientIndex++ % clientSize);
      if (Boolean.TRUE.equals(isClientAlive.get(clientIndex))) {
        return clientIndex;
      }
    }
    throw new PipeConnectionException(
        "All clients are dead, please check the connection to the receiver.");
  }

  @Override
  public void close() {
    for (int i = 0; i < clients.size(); ++i) {
      try {
        if (clients.get(i) != null) {
          clients.set(i, null).close();
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to close client {}.", i, e);
      } finally {
        isClientAlive.set(i, false);
      }
    }

    if (tabletBatchBuilder != null) {
      tabletBatchBuilder.close();
    }
  }
}
