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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;

public class ThriftCommonsSerDeUtilsTest {

  private static final ByteBuffer buffer = ByteBuffer.allocate(1024 * 10);

  @After
  public void cleanBuffer() {
    buffer.clear();
  }

  @Test
  public void readWriteTEndPointTest() throws IOException {
    TEndPoint endPoint0 = new TEndPoint("0.0.0.0", 6667);

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTEndPoint(endPoint0, outputStream);
      TEndPoint endPoint1 =
          ThriftCommonsSerDeUtils.deserializeTEndPoint(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(endPoint0, endPoint1);
    }
  }

  @Test
  public void readWriteTDataNodeConfigurationTest() throws IOException {
    TDataNodeLocation dataNodeLocation0 = new TDataNodeLocation();
    dataNodeLocation0.setDataNodeId(0);
    dataNodeLocation0.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation0.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation0.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation0.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation0.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    TNodeResource dataNodeResource0 = new TNodeResource();
    dataNodeResource0.setCpuCoreNum(16);
    dataNodeResource0.setMaxMemory(2022213861);

    TDataNodeConfiguration dataNodeConfiguration0 = new TDataNodeConfiguration();
    dataNodeConfiguration0.setLocation(dataNodeLocation0);
    dataNodeConfiguration0.setResource(dataNodeResource0);

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTDataNodeConfiguration(dataNodeConfiguration0, outputStream);
      TDataNodeConfiguration dataNodeConfiguration1 =
          ThriftCommonsSerDeUtils.deserializeTDataNodeConfiguration(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(dataNodeConfiguration0, dataNodeConfiguration1);
    }
  }

  @Test
  public void readWriteTDataNodeLocationTest() throws IOException {
    TDataNodeLocation dataNodeLocation0 = new TDataNodeLocation();
    dataNodeLocation0.setDataNodeId(0);
    dataNodeLocation0.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation0.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation0.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation0.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation0.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTDataNodeLocation(dataNodeLocation0, outputStream);
      TDataNodeLocation dataNodeLocation1 =
          ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(dataNodeLocation0, dataNodeLocation1);
    }
  }

  @Test
  public void readWriteTSeriesPartitionSlotTest() throws IOException {
    TSeriesPartitionSlot seriesPartitionSlot0 = new TSeriesPartitionSlot(10);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTSeriesPartitionSlot(seriesPartitionSlot0, outputStream);
      TSeriesPartitionSlot seriesPartitionSlot1 =
          ThriftCommonsSerDeUtils.deserializeTSeriesPartitionSlot(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(seriesPartitionSlot0, seriesPartitionSlot1);
    }
  }

  @Test
  public void writeTTimePartitionSlot() throws IOException {
    TTimePartitionSlot timePartitionSlot0 = new TTimePartitionSlot(100);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTTimePartitionSlot(timePartitionSlot0, outputStream);
      TTimePartitionSlot timePartitionSlot1 =
          ThriftCommonsSerDeUtils.deserializeTTimePartitionSlot(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(timePartitionSlot0, timePartitionSlot1);
    }
  }

  @Test
  public void readWriteTTimePartitionSlotListTest() throws IOException {
    TTimeSlotList timeSlotList0 = new TTimeSlotList();
    timeSlotList0.setTimePartitionSlots(new ArrayList<>());
    timeSlotList0.getTimePartitionSlots().add(new TTimePartitionSlot(100));
    timeSlotList0.getTimePartitionSlots().add(new TTimePartitionSlot(200));
    timeSlotList0.setNeedLeftAll(true);
    timeSlotList0.setNeedRightAll(false);

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTTimePartitionSlotList(timeSlotList0, outputStream);
      TTimeSlotList timeSlotList1 =
          ThriftCommonsSerDeUtils.deserializeTTimePartitionSlotList(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(timeSlotList0, timeSlotList1);
    }
  }

  @Test
  public void readWriteTConsensusGroupIdTest() throws IOException {
    TConsensusGroupId consensusGroupId0 =
        new TConsensusGroupId(TConsensusGroupType.ConfigRegion, 0);
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTConsensusGroupId(consensusGroupId0, outputStream);
      TConsensusGroupId consensusGroupId1 =
          ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(consensusGroupId0, consensusGroupId1);
    }
  }

  @Test
  public void readWriteTRegionReplicaSetTest() throws IOException {
    TRegionReplicaSet regionReplicaSet0 = new TRegionReplicaSet();
    regionReplicaSet0.setRegionId(new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 0));
    regionReplicaSet0.setDataNodeLocations(new ArrayList<>());
    for (int i = 0; i < 3; i++) {
      TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
      dataNodeLocation.setDataNodeId(i);
      dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667 + i));
      dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730 + i));
      dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740 + i));
      dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760 + i));
      dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750 + i));
      regionReplicaSet0.getDataNodeLocations().add(dataNodeLocation);
    }
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTRegionReplicaSet(regionReplicaSet0, outputStream);
      TRegionReplicaSet regionReplicaSet1 =
          ThriftCommonsSerDeUtils.deserializeTRegionReplicaSet(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(regionReplicaSet0, regionReplicaSet1);
    }
  }

  @Test
  public void readWriteTAINodeConfigurationTest() throws IOException {
    TAINodeLocation aiNodeLocation0 = new TAINodeLocation(0, new TEndPoint("0.0.0.0", 10810));

    TNodeResource aiNodeResource0 = new TNodeResource();
    aiNodeResource0.setCpuCoreNum(8);
    aiNodeResource0.setMaxMemory(1024L * 1024L * 1024L);

    TAINodeConfiguration aiNodeConfiguration0 =
        new TAINodeConfiguration(aiNodeLocation0, aiNodeResource0);

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTAINodeConfiguration(aiNodeConfiguration0, outputStream);
      TAINodeConfiguration aiNodeConfiguration1 =
          ThriftCommonsSerDeUtils.deserializeTAINodeConfiguration(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(aiNodeConfiguration0, aiNodeConfiguration1);
    }
  }

  @Test
  public void readWriteTAINodeLocationTest() throws IOException {
    TAINodeLocation aiNodeLocation0 = new TAINodeLocation(0, new TEndPoint("0.0.0.0", 10810));

    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ThriftCommonsSerDeUtils.serializeTAINodeLocation(aiNodeLocation0, outputStream);
      TAINodeLocation aiNodeLocation1 =
          ThriftCommonsSerDeUtils.deserializeTAINodeLocation(
              ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size()));
      Assert.assertEquals(aiNodeLocation0, aiNodeLocation1);
    }
  }

  @Test
  public void writeTAINodeConfigurationFailureUsesAINodeMessage() {
    TAINodeConfiguration aiNodeConfiguration =
        new TAINodeConfiguration(
            new TAINodeLocation(0, new TEndPoint("0.0.0.0", 10810)), new TNodeResource(8, 1024));

    ThriftSerDeException exception =
        Assert.assertThrows(
            ThriftSerDeException.class,
            () ->
                ThriftCommonsSerDeUtils.serializeTAINodeConfiguration(
                    aiNodeConfiguration, new DataOutputStream(new FailingOutputStream())));

    Assert.assertTrue(
        exception.getMessage(), exception.getMessage().contains("TAINodeConfiguration"));
    Assert.assertFalse(
        exception.getMessage(), exception.getMessage().contains("TDataNodeConfiguration"));
  }

  @Test
  public void writeTDataNodeInfoFailureUsesDataNodeConfigurationMessage() {
    TDataNodeConfiguration dataNodeConfiguration = new TDataNodeConfiguration();
    TDataNodeLocation dataNodeLocation = new TDataNodeLocation();
    dataNodeLocation.setDataNodeId(0);
    dataNodeLocation.setClientRpcEndPoint(new TEndPoint("0.0.0.0", 6667));
    dataNodeLocation.setInternalEndPoint(new TEndPoint("0.0.0.0", 10730));
    dataNodeLocation.setMPPDataExchangeEndPoint(new TEndPoint("0.0.0.0", 10740));
    dataNodeLocation.setDataRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10760));
    dataNodeLocation.setSchemaRegionConsensusEndPoint(new TEndPoint("0.0.0.0", 10750));
    dataNodeConfiguration.setLocation(dataNodeLocation);
    dataNodeConfiguration.setResource(new TNodeResource(16, 1024));

    ThriftSerDeException exception =
        Assert.assertThrows(
            ThriftSerDeException.class,
            () ->
                ThriftCommonsSerDeUtils.serializeTDataNodeInfo(
                    dataNodeConfiguration, new DataOutputStream(new FailingOutputStream())));

    Assert.assertTrue(
        exception.getMessage(), exception.getMessage().contains("TDataNodeConfiguration"));
    Assert.assertFalse(exception.getMessage(), exception.getMessage().contains("TDataNodeInfo"));
  }

  @Test
  public void readTDataNodeInfoFailureUsesDataNodeConfigurationMessage() {
    ThriftSerDeException exception =
        Assert.assertThrows(
            ThriftSerDeException.class,
            () -> ThriftCommonsSerDeUtils.deserializeTDataNodeInfo(ByteBuffer.allocate(0)));

    Assert.assertTrue(
        exception.getMessage(), exception.getMessage().contains("TDataNodeConfiguration"));
    Assert.assertFalse(exception.getMessage(), exception.getMessage().contains("TDataNodeInfo"));
  }

  @Test
  public void writeTAINodeInfoFailureUsesAINodeConfigurationMessage() {
    TAINodeConfiguration aiNodeConfiguration =
        new TAINodeConfiguration(
            new TAINodeLocation(0, new TEndPoint("0.0.0.0", 10810)), new TNodeResource(8, 1024));

    ThriftSerDeException exception =
        Assert.assertThrows(
            ThriftSerDeException.class,
            () ->
                ThriftCommonsSerDeUtils.serializeTAINodeInfo(
                    aiNodeConfiguration, new DataOutputStream(new FailingOutputStream())));

    Assert.assertTrue(
        exception.getMessage(), exception.getMessage().contains("TAINodeConfiguration"));
    Assert.assertFalse(exception.getMessage(), exception.getMessage().contains("TAINodeInfo"));
  }

  @Test
  public void readTAINodeInfoFailureUsesAINodeConfigurationMessage() {
    ThriftSerDeException exception =
        Assert.assertThrows(
            ThriftSerDeException.class,
            () -> ThriftCommonsSerDeUtils.deserializeTAINodeInfo(ByteBuffer.allocate(0)));

    Assert.assertTrue(
        exception.getMessage(), exception.getMessage().contains("TAINodeConfiguration"));
    Assert.assertFalse(exception.getMessage(), exception.getMessage().contains("TAINodeInfo"));
  }

  @Test
  public void readTAINodeLocationFailureUsesAINodeMessage() {
    ThriftSerDeException exception =
        Assert.assertThrows(
            ThriftSerDeException.class,
            () -> ThriftCommonsSerDeUtils.deserializeTAINodeLocation(ByteBuffer.allocate(0)));

    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("TAINodeLocation"));
    Assert.assertFalse(
        exception.getMessage(), exception.getMessage().contains("TDataNodeLocation"));
  }

  private static class FailingOutputStream extends OutputStream {

    @Override
    public void write(int b) throws IOException {
      throw new IOException("forced failure");
    }
  }
}
