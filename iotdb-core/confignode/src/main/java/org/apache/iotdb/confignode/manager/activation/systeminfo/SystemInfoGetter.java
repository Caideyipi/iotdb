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

package org.apache.iotdb.confignode.manager.activation.systeminfo;

import org.slf4j.Logger;

import java.io.IOException;

public abstract class SystemInfoGetter implements ISystemInfoGetter {

  abstract Logger getLogger();

  private static final String GET_SYSTEM_INFO_FAIL = "Get system info fail.";

  abstract String getCPUIdImpl() throws IOException;

  public String getCPUId() {
    try {
      return getCPUIdImpl();
    } catch (Exception e) {
      //      getLogger().warn(GET_SYSTEM_INFO_FAIL);
      //      return "";
      throw new RuntimeException();
    }
  }

  abstract String getMainBoardIdImpl() throws IOException;

  public String getMainBoardId() {
    try {
      return getMainBoardIdImpl();
    } catch (Exception e) {
      //      getLogger().warn(GET_SYSTEM_INFO_FAIL);
      //      return "";
      throw new RuntimeException(e);
    }
  }

  abstract String getSystemUUIDImpl() throws IOException;

  public String getSystemUUID() {
    try {
      return getSystemUUIDImpl();
    } catch (Exception e) {
      //      getLogger().warn(GET_SYSTEM_INFO_FAIL);
      //      return "";
      throw new RuntimeException(e);
    }
  }
}
