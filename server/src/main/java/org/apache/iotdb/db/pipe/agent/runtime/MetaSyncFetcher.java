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

package org.apache.iotdb.db.pipe.agent.runtime;

import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TSyncPipeMetaResp;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeClientManager;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetaSyncFetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(MetaSyncFetcher.class);

  private final IClientManager<ConfigRegionId, ConfigNodeClient> configNodeClientManager =
      ConfigNodeClientManager.getInstance();

  private Future<?> currentHeartbeatFuture;
  private final ScheduledExecutorService heartBeatExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PIPE_META_SYNC_EXECUTOR_POOL.getName());

  private ConfigNodeClient client;

  /** Execute the metaSync service */
  public void executeMetaSync() {
    try {
      // Borrow client each time in case previous borrow failed
      if (client == null) {
        client = configNodeClientManager.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID);
      }
      final TSyncPipeMetaResp syncPipeMetaResp = client.syncPipeMeta();
      if (TSStatusCode.SUCCESS_STATUS.getStatusCode() != syncPipeMetaResp.getStatus().getCode()) {
        LOGGER.warn("[{}] Failed to sync pipe meta.", syncPipeMetaResp.getStatus());
      } else {
        // TODO: update the pipeMeta on the DataNode
      }
    } catch (ClientManagerException | TException e) {
      LOGGER.warn("MetaSync service failed because {}.", e.getMessage());
    }
  }

  /** Start the metaSync service */
  public void startMetaSyncService() {
    if (currentHeartbeatFuture == null) {
      currentHeartbeatFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              heartBeatExecutor,
              this::executeMetaSync,
              0,
              IoTDBDescriptor.getInstance().getConfig().getPipeSyncInterval(),
              TimeUnit.SECONDS);
      LOGGER.info("MetaSync service is started successfully.");
    }
  }

  /** Stop the metaSync service */
  public void stopMetaSyncService() {
    if (currentHeartbeatFuture != null) {
      currentHeartbeatFuture.cancel(false);
      currentHeartbeatFuture = null;
      LOGGER.info("MetaSync service is stopped successfully.");
    }
  }
}
