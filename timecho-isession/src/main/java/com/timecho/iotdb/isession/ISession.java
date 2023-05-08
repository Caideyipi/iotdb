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
package com.timecho.iotdb.isession;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.LicenseInfoResp;
import org.apache.iotdb.service.rpc.thrift.WhiteListInfoResp;

import java.util.Set;

public interface ISession extends org.apache.iotdb.isession.ISession {
  WhiteListInfoResp getWhiteIpSet() throws IoTDBConnectionException, StatementExecutionException;

  void updateWhiteList(Set<String> ipSet)
      throws IoTDBConnectionException, StatementExecutionException;

  LicenseInfoResp getLicenseInfo() throws StatementExecutionException, IoTDBConnectionException;

  /**
   * Queries the total number of points of a specified database. If the dataset parameter is empty
   * or not passed, returns the total number of points of all databases. This method counts the
   * points in the tsfile file, excluding the points in memory.
   */
  long getTotalPoints(Set<String> databaseSet)
      throws StatementExecutionException, IoTDBConnectionException;

  /**
   * Similar to the above method, start and end timestamp parameters are added to filter the data *
   */
  long getTotalPoints(Set<String> databaseSet, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException;
  /** Similar to the above method, start timestamp parameters are added to filter the data * */
  long getTotalPoints(Set<String> databaseSet, long startTime)
      throws StatementExecutionException, IoTDBConnectionException;
}
