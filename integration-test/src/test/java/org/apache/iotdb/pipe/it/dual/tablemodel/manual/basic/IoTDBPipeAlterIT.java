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

package org.apache.iotdb.pipe.it.dual.tablemodel.manual.basic;

import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowPipeReq;
import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.MultiClusterIT2DualTableManualBasic;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.it.dual.tablemodel.TableModelUtils;
import org.apache.iotdb.pipe.it.dual.tablemodel.manual.AbstractPipeTableModelDualManualIT;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({MultiClusterIT2DualTableManualBasic.class})
public class IoTDBPipeAlterIT extends AbstractPipeTableModelDualManualIT {

  @Override
  @Before
  public void setUp() {
    super.setUp();
  }

  @Test
  public void testBasicAlterPipe() throws Exception {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // Create pipe
    // The database & table name will be converted to lower case
    final String sql =
        String.format(
            "create pipe a2b with source ('source'='iotdb-source', 'database-name'='Test', 'table-name'='Test1', 'mode.streaming'='true') with processor ('processor'='do-nothing-processor') with sink ('node-urls'='%s')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    long lastCreationTime;
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setIsTableModel(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // Check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("database-name=test"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("table-name=test"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("mode.streaming=true"));
      Assert.assertTrue(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Record last creation time
      lastCreationTime = showPipeResult.get(0).creationTime;
    }

    // Stop pipe
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("stop pipe a2b");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setIsTableModel(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
    }

    // Alter pipe (modify)
    // The database & table name will be converted to lower case
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "alter pipe a2b modify source ('table-name'='Test1','database-name'='Test1')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setIsTableModel(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
      // Check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("table-name=test1"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("database-name=test1"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("mode.streaming=true"));
      Assert.assertTrue(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (replace)
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "alter pipe a2b replace source ('capture.table'='true','''source'='iotdb-source', 'table-name'='test','database-name'='test')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setIsTableModel(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
      // check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("database-name=test"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("table-name=test"));
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("mode.streaming=true"));
      Assert.assertTrue(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (modify)
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify sink ('sink.batch.enable'='false')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setIsTableModel(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("STOPPED", showPipeResult.get(0).state);
      // Check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("database-name=test"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("table-name=test"));
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=false"));
      Assert.assertTrue(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Start pipe
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("start pipe a2b");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Alter pipe (modify)
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify sink ('connector.batch.enable'='true')");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setIsTableModel(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // Check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("database-name=test"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("table-name=test"));
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=true"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (modify empty)
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify source ()");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setIsTableModel(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("database-name=test"));
      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("table-name=test"));
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=true"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (replace empty)
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b replace source ()");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setIsTableModel(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations

      Assert.assertTrue(showPipeResult.get(0).pipeExtractor.contains("__system.sql-dialect=table"));
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("source=iotdb-source"));
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("database-name=test"));
      Assert.assertFalse(showPipeResult.get(0).pipeExtractor.contains("table-name=test"));
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=true"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (replace empty)
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b replace processor ()");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setIsTableModel(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=true"));
      Assert.assertFalse(
          showPipeResult.get(0).pipeProcessor.contains("processor=do-nothing-processor"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      lastCreationTime = showPipeResult.get(0).creationTime;
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }

    // Alter pipe (modify empty)
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute("alter pipe a2b modify sink ()");
    } catch (SQLException e) {
      fail(e.getMessage());
    }

    // Show pipe
    try (final SyncConfigNodeIServiceClient client =
        (SyncConfigNodeIServiceClient) senderEnv.getLeaderConfigNodeConnection()) {
      final List<TShowPipeInfo> showPipeResult =
          client.showPipe(new TShowPipeReq().setIsTableModel(true)).pipeInfoList;
      showPipeResult.removeIf(i -> i.getId().startsWith("__consensus"));
      Assert.assertEquals(1, showPipeResult.size());
      // Check status
      Assert.assertEquals("RUNNING", showPipeResult.get(0).state);
      // Check configurations
      Assert.assertTrue(showPipeResult.get(0).pipeConnector.contains("batch.enable=true"));
      Assert.assertTrue(
          showPipeResult
              .get(0)
              .pipeConnector
              .contains(String.format("node-urls=%s", receiverDataNode.getIpAndPortString())));
      // Check creation time and record last creation time
      Assert.assertTrue(showPipeResult.get(0).creationTime > lastCreationTime);
      // Check exception message
      Assert.assertEquals("", showPipeResult.get(0).exceptionMessage);
    }
  }

  @Test
  public void testAlterPipeFailure() {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);

    // alter non-existed pipe
    String sql =
        String.format(
            "alter pipe a2b modify sink ('node-urls'='%s', 'batch.enable'='true')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
      fail();
    } catch (SQLException ignore) {
    }

    // Create pipe
    sql =
        String.format(
            "create pipe a2b with source ('source'='iotdb-source', 'database-name'='test', 'table-name'='test1', 'mode.streaming'='true') with sink ('node-urls'='%s', 'batch.enable'='false')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testAlterPipeSourceAndSink() {
    final DataNodeWrapper receiverDataNode = receiverEnv.getDataNodeWrapper(0);
    boolean insertResult = true;

    final Consumer<String> handleFailure =
        o -> {
          TestUtils.executeNonQueryWithRetry(senderEnv, "flush");
          TestUtils.executeNonQueryWithRetry(receiverEnv, "flush");
        };

    TableModelUtils.createDataBaseAndTable(senderEnv, "test", "test");
    TableModelUtils.createDataBaseAndTable(senderEnv, "test1", "test1");
    // Create pipe
    final String sql =
        String.format(
            "create pipe a2b with source ('source'='iotdb-source', 'database-name'='test', 'table-name'='test', 'mode.streaming'='true') with processor ('processor'='do-nothing-processor') with sink ('node-urls'='%s', 'batch.enable'='false')",
            receiverDataNode.getIpAndPortString());
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (final SQLException e) {
      fail(e.getMessage());
    }

    insertResult = TableModelUtils.insertData("test", "test", 0, 100, senderEnv);
    insertResult = insertResult && TableModelUtils.insertData("test1", "test1", 0, 100, senderEnv);
    if (!insertResult) {
      return;
    }

    // Check data on receiver
    TableModelUtils.assertData("test", "test", 0, 100, receiverEnv, handleFailure);

    // Alter pipe (modify 'source.path', 'source.inclusion' and
    // 'processor.tumbling-time.interval-seconds')
    try (final Connection connection = senderEnv.getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      statement.execute(
          "alter pipe a2b modify source('source' = 'iotdb-source','database-name'='test1', 'table-name'='test1', 'mode.streaming'='true', 'source.inclusion'='data.insert') modify sink ('batch.enable'='true')");
    } catch (final SQLException e) {
      fail(e.getMessage());
    }
    insertResult = TableModelUtils.insertData("test", "test", 100, 200, senderEnv);
    insertResult =
        insertResult && TableModelUtils.insertData("test1", "test1", 100, 200, senderEnv);
    if (!insertResult) {
      return;
    }
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        TableModelUtils.getQuerySql("test"),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(0, 100),
        "test",
        handleFailure);
    TestUtils.assertDataEventuallyOnEnv(
        receiverEnv,
        TableModelUtils.getQuerySql("test1"),
        TableModelUtils.generateHeaderResults(),
        TableModelUtils.generateExpectedResults(0, 200),
        "test1",
        handleFailure);
  }
}
