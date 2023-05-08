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
package com.timecho.iotdb.rpc;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.external.api.thrift.JudgableServerContext;

import java.io.IOException;
import java.net.Socket;

public class WhiteListServerContext implements JudgableServerContext {
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  Socket client;

  public WhiteListServerContext(Socket socket) {
    this.client = socket;
  }

  @Override
  public boolean whenConnect() {
    if (config.isEnableWhiteList()
        && !IPFilter.isInWhiteList(client.getInetAddress().getHostAddress())) {
      try {
        client.close();
        return false;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return true;
  }

  @Override
  public boolean whenDisconnect() {
    return true;
  }

  @Override
  public <T> T unwrap(Class<T> iface) {
    return JudgableServerContext.super.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return JudgableServerContext.super.isWrapperFor(iface);
  }
}
