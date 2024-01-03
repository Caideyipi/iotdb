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

package org.apache.iotdb.confignode.manager.pipe.transfer.connector.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftSyncClientManager;
import org.apache.iotdb.confignode.manager.pipe.transfer.connector.payload.request.PipeTransferConfigNodeHandshakeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;
import java.util.List;

public class IoTDBThriftSyncClientConfigNodeManager extends IoTDBThriftSyncClientManager {

  public IoTDBThriftSyncClientConfigNodeManager(
      List<TEndPoint> endPoints, boolean useSSL, String trustStorePath, String trustStorePwd) {
    super(endPoints, useSSL, trustStorePath, trustStorePwd);
  }

  @Override
  protected TPipeTransferReq buildHandShakeReq() throws IOException {
    return PipeTransferConfigNodeHandshakeReq.toTPipeTransferReq(
        CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }
}
