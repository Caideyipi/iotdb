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

package org.apache.iotdb.subscription.it.triple.regression.pullconsumer.loose_range;

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2SubscriptionRegression;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPullConsumer;
import org.apache.iotdb.subscription.it.triple.regression.AbstractSubscriptionRegressionIT;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/***
 * PullConsumer
 * pattern: ts
 * format: tsfile
 * loose-range: all
 * mode: snapshot
 */
@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2SubscriptionRegression.class})
public class IoTDBAllTsfilePullConsumerSnapshotIT extends AbstractSubscriptionRegressionIT {
  private static final String database = "root.AllTsfilePullConsumerSnapshot";
  private static final String database2 = "root.test.AllTsfilePullConsumerSnapshot";
  private static final String device = database + ".d_0";
  private static final String topicName = "TopicAllTsfilePullConsumerSnapshot";
  private static final String pattern = device + ".s_0";
  private static final String device2 = database + ".d_1";
  private static SubscriptionPullConsumer consumer;
  private List<MeasurementSchema> schemaList = new ArrayList<>();
  private long nowTimestamp;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    createDB(database);
    nowTimestamp = System.currentTimeMillis();
    createTopic_s(
        topicName,
        pattern,
        null,
        String.valueOf(nowTimestamp),
        true,
        TopicConstant.MODE_SNAPSHOT_VALUE,
        TopicConstant.LOOSE_RANGE_ALL_VALUE);
    session_src.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_src.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_dest.createTimeseries(
        device + ".s_0", TSDataType.INT64, TSEncoding.GORILLA, CompressionType.LZ4);
    session_dest.createTimeseries(
        device + ".s_1", TSDataType.DOUBLE, TSEncoding.TS_2DIFF, CompressionType.LZMA2);
    session_src.executeNonQueryStatement(
        "create aligned timeseries " + device2 + "(s_0 int64,s_1 double);");
    session_dest.executeNonQueryStatement(
        "create aligned timeseries " + device2 + "(s_0 int64,s_1 double);");
    session_src.executeNonQueryStatement("create timeseries " + database2 + ".d_2.s_0 int32;");
    session_dest.executeNonQueryStatement("create timeseries " + database2 + ".d_2.s_0 int32;");
    session_src.executeNonQueryStatement("create timeseries " + database2 + ".d_2.s_1 double;");
    session_dest.executeNonQueryStatement("create timeseries " + database2 + ".d_2.s_1 double;");
    session_src.executeNonQueryStatement(
        "insert into " + database2 + ".d_2(time,s_0,s_1)values(1000,132,4567.89);");
    session_src.executeNonQueryStatement(
        "insert into " + device2 + "(time,s_0,s_1)values(2000,232,567.891);");
    schemaList.add(new MeasurementSchema("s_0", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s_1", TSDataType.DOUBLE));
    subs.getTopics().forEach((System.out::println));
    assertTrue(subs.getTopic(topicName).isPresent(), "Create show topics");
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      consumer.close();
    } catch (Exception e) {
    }
    subs.dropTopic(topicName);
    dropDB(database);
    dropDB(database2);
    super.tearDown();
  }

  private void insert_data(long timestamp, String device)
      throws IoTDBConnectionException, StatementExecutionException, InterruptedException {
    Tablet tablet = new Tablet(device, schemaList, 5);
    int rowIndex = 0;
    for (int row = 0; row < 5; row++) {
      rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue("s_0", rowIndex, (row + 1) * 20L + row);
      tablet.addValue("s_1", rowIndex, row + 2.45);
      timestamp += 2000;
    }
    session_src.insertTablet(tablet);
    session_src.executeNonQueryStatement("flush;");
  }

  @Test
  public void do_test()
      throws InterruptedException,
          TException,
          IoTDBConnectionException,
          IOException,
          StatementExecutionException {
    // Write data before subscribing
    insert_data(1706659200000L, device); // 2024-01-31 08:00:00+08:00
    insert_data(1706659200000L, device2); // 2024-01-31 08:00:00+08:00

    consumer =
        new SubscriptionPullConsumer.Builder()
            .host(SRC_HOST)
            .port(SRC_PORT)
            .consumerId("snapshot_ts_pattern_tsfile_pull")
            .consumerGroupId("loose_range_all")
            .autoCommit(false)
            .fileSaveDir("target/pull-subscription") // hack for license check
            .buildPullConsumer();
    consumer.open();
    // Subscribe
    consumer.subscribe(topicName);
    subs.getSubscriptions().forEach(System.out::println);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after subscription");

    insert_data(nowTimestamp - 4000, device);
    insert_data(nowTimestamp - 4000, device2);
    System.out.println(
        FORMAT.format(new Date())
            + " src filter:"
            + getCount(
                session_src, "select count(s_0) from " + device + " where time <=" + nowTimestamp));

    // Consumption data
    List<String> paths = new ArrayList<>(3);
    paths.add(device);
    paths.add(device2);
    paths.add(database2 + ".d_2");
    List<Integer> rowCountList = consume_tsfile(consumer, paths);
    // Subscribe and write without consuming
    assertEquals(rowCountList.get(0), 5, "Write without consume after subscription");
    assertEquals(rowCountList.get(1), 0);
    assertEquals(rowCountList.get(2), 0);

    // Unsubscribe
    consumer.unsubscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 0, "After cancellation, show subscriptions");

    // Subscribe and then write data
    consumer.subscribe(topicName);
    assertEquals(subs.getSubscriptions().size(), 1, "show subscriptions after re-subscribing");
    insert_data(1707782400000L, device); // 2024-02-13 08:00:00+08:00
    insert_data(1707782400000L, device2); // 2024-02-13 08:00:00+08:00

    // Consumption data: Progress is not retained after unsubscribing and re-subscribing. Full
    // synchronization.
    rowCountList = consume_tsfile(consumer, paths);
    assertEquals(
        rowCountList.get(0),
        10,
        "Unsubscribe and then resubscribe, progress is not retained. Full synchronization.");
    assertEquals(rowCountList.get(1), 0, "Unsubscribe and then resubscribe," + device2);
    assertEquals(rowCountList.get(2), 0, "Unsubscribe and then resubscribe," + database2 + ".d_2");
  }
}
