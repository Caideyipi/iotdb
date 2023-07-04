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

package org.apache.iotdb.confignode.client.async.handlers.rpc;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.mpp.rpc.thrift.TPushPipeMetaResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class PushPipeMetaRPCHandler extends AbstractAsyncRPCHandler<TPushPipeMetaResp> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PushPipeMetaRPCHandler.class);

  public PushPipeMetaRPCHandler(
      DataNodeRequestType requestType,
      int requestId,
      TDataNodeLocation targetDataNode,
      Map<Integer, TDataNodeLocation> dataNodeLocationMap,
      Map<Integer, TPushPipeMetaResp> responseMap,
      CountDownLatch countDownLatch) {
    super(requestType, requestId, targetDataNode, dataNodeLocationMap, responseMap, countDownLatch);
  }

  @Override
  public void onComplete(TPushPipeMetaResp response) {
    // Put response
    responseMap.put(requestId, response);

    if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.info("Successfully {} on DataNode: {}", requestType, formattedTargetLocation);
    } else {
      LOGGER.error(
          "Failed to {} on DataNode: {}, response: {}",
          requestType,
          formattedTargetLocation,
          response);
    }

    // Always remove to avoid retrying
    dataNodeLocationMap.remove(requestId);

    // Always CountDown
    countDownLatch.countDown();
  }

  @Override
  public void onError(Exception e) {
    String errorMsg =
        "Failed to "
            + requestType
            + " on DataNode: "
            + formattedTargetLocation
            + ", exception: "
            + e.getMessage();
    LOGGER.error(errorMsg, e);

    responseMap.put(
        requestId,
        new TPushPipeMetaResp(RpcUtils.getStatus(TSStatusCode.PUSH_PIPE_META_ERROR, errorMsg)));

    // Always CountDown
    countDownLatch.countDown();
  }
}
