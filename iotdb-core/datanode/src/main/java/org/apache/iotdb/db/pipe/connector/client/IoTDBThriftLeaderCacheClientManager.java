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

package org.apache.iotdb.db.pipe.connector.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.pipe.connector.client.IoTDBThriftClientManager;

import java.util.List;

abstract class IoTDBThriftLeaderCacheClientManager extends IoTDBThriftClientManager {

  protected final boolean useLeaderCache;
  protected static final LeaderCacheManager leaderCacheManager = new LeaderCacheManager();

  protected IoTDBThriftLeaderCacheClientManager(
      List<TEndPoint> endPointList, boolean useLeaderCache) {
    super(endPointList);
    this.useLeaderCache = useLeaderCache;
  }
}
