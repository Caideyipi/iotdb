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
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2.class})
public class IoTDBPipeMetaLeaderChangeIT extends AbstractPipeDualManualIT {
  @Override
  @Before
  public void setUp() {
    try {
      MultiEnvFactory.createEnv(2);
      senderEnv = MultiEnvFactory.getEnv(0);
      receiverEnv = MultiEnvFactory.getEnv(1);

      senderEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(false);
      receiverEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(false);

      senderEnv
          .getConfig()
          .getCommonConfig()
          .setAutoCreateSchemaEnabled(true)
          .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
          .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
          .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
          .setSchemaReplicationFactor(3);

      senderEnv.initClusterEnvironment(3, 3, 180);
      receiverEnv.initClusterEnvironment();
    } catch (Throwable e) {
      Assume.assumeNoException(e);
    }
  }

  @Test
  public void testConfigNodeLeaderChange() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.inclusion.exclusion", "");
      extractorAttributes.put("extractor.forwarding-pipe-requests", "false");
      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    int successCount = 0;
    for (int i = 0; i < 10; ++i) {
      if (TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("create database root.ln%s", i))) {
        ++successCount;
      }
    }

    try {
      senderEnv.shutdownConfigNode(senderEnv.getLeaderConfigNodeIndex());
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    for (int i = 10; i < 20; ++i) {
      if (TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv, String.format("create database root.ln%s", i))) {
        ++successCount;
      }
    }

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count databases",
        "count,",
        Collections.singleton(String.format("%d,", successCount)));
  }

  @Test
  public void testSchemaRegionLeaderChange() throws Exception {
    DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    String receiverIp = receiverDataNode.getIp();
    int receiverPort = receiverDataNode.getPort();

    try (SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      Map<String, String> extractorAttributes = new HashMap<>();
      Map<String, String> processorAttributes = new HashMap<>();
      Map<String, String> connectorAttributes = new HashMap<>();

      extractorAttributes.put("extractor.inclusion", "all");
      extractorAttributes.put("extractor.inclusion.exclusion", "");
      extractorAttributes.put("extractor.forwarding-pipe-requests", "false");
      connectorAttributes.put("connector", "iotdb-thrift-connector");
      connectorAttributes.put("connector.ip", receiverIp);
      connectorAttributes.put("connector.port", Integer.toString(receiverPort));
      connectorAttributes.put("connector.exception.conflict.resolve-strategy", "retry");
      connectorAttributes.put("connector.exception.conflict.retry-max-time-seconds", "-1");

      TSStatus status =
          client.createPipe(
              new TCreatePipeReq("testPipe", connectorAttributes)
                  .setExtractorAttributes(extractorAttributes)
                  .setProcessorAttributes(processorAttributes));

      Assert.assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());

      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), client.startPipe("testPipe").getCode());
    }

    int successCount = 0;
    for (int i = 0; i < 10; ++i) {
      if (TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv,
          String.format(
              "create timeseries root.ln.wf01.GPS.status%s with datatype=BOOLEAN,encoding=PLAIN",
              i))) {
        ++successCount;
      }
    }

    // We do not test the schema region's recover process since
    // it hasn't been implemented yet in simple consensus
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count timeseries",
        "count(timeseries),",
        Collections.singleton(String.format("%d,", successCount)));

    try {
      senderEnv.shutdownDataNode(senderEnv.getFirstLeaderSchemaRegionDataNodeIndex());
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }

    for (int i = 10; i < 20; ++i) {
      if (TestUtils.tryExecuteNonQueryWithRetry(
          senderEnv,
          String.format(
              "create timeseries root.ln.wf01.GPS.status%s with datatype=BOOLEAN,encoding=PLAIN",
              i))) {
        ++successCount;
      }
    }

    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        "count timeseries",
        "count(timeseries),",
        Collections.singleton(String.format("%d,", successCount)));
  }
}
