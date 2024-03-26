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

package org.apache.iotdb.pipe.it.manual;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
public class IoTDBPipeMetaHistoricalIT extends AbstractPipeDualManualIT {
  @Test
  public void testPureSchemaInclusion() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "create database root.ln",
              "set ttl to root.ln 3600000",
              "create user `thulab` 'passwd'",
              "create role `admin`",
              "grant role `admin` to `thulab`",
              "grant read on root.** to role `admin`",
              "create schema template t1 (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)",
              "set schema template t1 to root.ln.wf01",
              "create timeseries using schema template on root.ln.wf01.wt01",
              "create timeseries root.ln.wf02.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (0, 23, true)"))) {
        return;
      }

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "data, schema");
      extractorAttributes.put(
          "extractor.inclusion.exclusion", "schema.timeseries.ordinary, schema.ttl");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "list user", "user,", Collections.singleton("root,"));
      TestUtils.assertDataAlwaysOnEnv(receiverEnv, "list role", "role,", Collections.emptySet());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "show databases",
          "Database,TTL,SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.singleton("root.ln,null,3,2,604800000,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "select * from root.**",
          "Time,root.ln.wf01.temperature,root.ln.wf01.status,",
          Collections.singleton("0,23.0,true,"));

      if (!TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, "create timeseries using schema template on root.ln.wf01.wt02")) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "count timeseries", "count(timeseries),", Collections.singleton("4,"));
    }
  }

  @Test
  public void testAuthInclusion() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {

      // Do not fail if the failure has nothing to do with pipe
      // Because the failures will randomly generate due to resource limitation
      if (!TestUtils.tryExecuteNonQueriesWithRetry(
          senderEnv,
          Arrays.asList(
              "create database root.ln",
              "set ttl to root.ln 3600000",
              "create user `thulab` 'passwd'",
              "create role `admin`",
              "grant role `admin` to `thulab`",
              "grant read on root.** to role `admin`",
              "create schema template t1 (temperature FLOAT encoding=RLE, status BOOLEAN encoding=PLAIN compression=SNAPPY)",
              "set schema template t1 to root.ln.wf01",
              "create timeseries using schema template on root.ln.wf01.wt01",
              "create timeseries root.ln.wf02.wt01.status with datatype=BOOLEAN,encoding=PLAIN",
              "insert into root.ln.wf01.wt01(time, temperature, status) values (0, 23, true)"))) {
        return;
      }

      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "auth");

      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "list user of role `admin`", "user,", Collections.singleton("thulab,"));
      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv,
          "list privileges of role `admin`",
          "ROLE,PATH,PRIVILEGES,GRANT OPTION,",
          new HashSet<>(
              Arrays.asList("admin,root.**,READ_DATA,false,", "admin,root.**,READ_SCHEMA,false,")));

      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv,
          "show databases",
          "Database,TTL,SchemaReplicationFactor,DataReplicationFactor,TimePartitionInterval,",
          Collections.emptySet());
      TestUtils.assertDataAlwaysOnEnv(
          receiverEnv, "select * from root.**", "Time", Collections.emptySet());

      if (!TestUtils.tryExecuteNonQueryWithRetry(senderEnv, "CREATE ROLE test")) {
        return;
      }

      TestUtils.assertDataEventuallyOnEnv(
          receiverEnv, "list role", "role,", new HashSet<>(Arrays.asList("admin,", "test,")));
    }
  }
}
