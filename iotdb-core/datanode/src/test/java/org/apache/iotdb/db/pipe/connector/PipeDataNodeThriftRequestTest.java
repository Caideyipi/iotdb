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

package org.apache.iotdb.db.pipe.connector;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.connector.payload.response.PipeTransferFilePieceResp;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferDataNodeHandshakeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaPlanReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotPieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferSchemaSnapshotSealReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletBinaryReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletInsertNodeReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTabletRawReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFilePieceReq;
import org.apache.iotdb.db.pipe.connector.payload.evolvable.request.PipeTransferTsFileSealReq;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metedata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PipeDataNodeThriftRequestTest {

  private static final String TIME_PRECISION = "ms";

  @Test
  public void testPipeTransferDataNodeHandshakeReq() throws IOException {
    PipeTransferDataNodeHandshakeReq req =
        PipeTransferDataNodeHandshakeReq.toTPipeTransferReq(TIME_PRECISION);
    PipeTransferDataNodeHandshakeReq deserializeReq =
        PipeTransferDataNodeHandshakeReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getTimestampPrecision(), deserializeReq.getTimestampPrecision());
  }

  @Test
  public void testPipeTransferInsertNodeReq() {
    PipeTransferTabletInsertNodeReq req =
        PipeTransferTabletInsertNodeReq.toTPipeTransferReq(
            new InsertRowNode(
                new PlanNodeId(""),
                new PartialPath(new String[] {"root", "sg", "d"}),
                false,
                new String[] {"s"},
                new TSDataType[] {TSDataType.INT32},
                1,
                new Object[] {1},
                false));
    PipeTransferTabletInsertNodeReq deserializeReq =
        PipeTransferTabletInsertNodeReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getInsertNode(), deserializeReq.getInsertNode());

    Statement statement = req.constructStatement();
    List<PartialPath> paths = new ArrayList<>();
    paths.add(new PartialPath(new String[] {"root", "sg", "d", "s"}));
    Assert.assertEquals(statement.getPaths(), paths);
  }

  @Test
  public void testPipeTransferTabletBinaryReq() {
    // Not do real test here since "serializeToWal" needs private inner class of walBuffer
    PipeTransferTabletBinaryReq req =
        PipeTransferTabletBinaryReq.toTPipeTransferReq(ByteBuffer.wrap(new byte[] {'a', 'b'}));
    PipeTransferTabletBinaryReq deserializeReq =
        PipeTransferTabletBinaryReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());
  }

  @Test
  public void testPipeTransferSchemaPlanReq() {
    PipeTransferSchemaPlanReq req =
        PipeTransferSchemaPlanReq.toTPipeTransferReq(
            new CreateAlignedTimeSeriesNode(
                new PlanNodeId(""),
                new PartialPath(new String[] {"root", "sg", "d"}),
                Collections.singletonList("s"),
                Collections.singletonList(TSDataType.INT32),
                Collections.singletonList(TSEncoding.PLAIN),
                Collections.singletonList(CompressionType.UNCOMPRESSED),
                null,
                null,
                null));

    PipeTransferSchemaPlanReq deserializeReq = PipeTransferSchemaPlanReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getPlanNode(), deserializeReq.getPlanNode());
  }

  @Test
  public void testPipeTransferTabletReq() {
    try {
      List<MeasurementSchema> schemaList = new ArrayList<>();
      schemaList.add(new MeasurementSchema("s1", TSDataType.INT32));
      schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
      schemaList.add(new MeasurementSchema("s3", TSDataType.FLOAT));
      schemaList.add(new MeasurementSchema("s4", TSDataType.DOUBLE));
      schemaList.add(new MeasurementSchema("s5", TSDataType.BOOLEAN));
      schemaList.add(new MeasurementSchema("s6", TSDataType.TEXT));
      Tablet t = new Tablet("root.sg.d", schemaList, 1024);
      t.rowSize = 2;
      t.addTimestamp(0, 2000);
      t.addTimestamp(1, 1000);
      t.addValue("s1", 0, 2);
      t.addValue("s6", 0, "2");
      t.addValue("s1", 1, 1);
      t.addValue("s6", 1, "1");
      PipeTransferTabletRawReq req = PipeTransferTabletRawReq.toTPipeTransferReq(t, false);
      PipeTransferTabletRawReq deserializeReq = PipeTransferTabletRawReq.fromTPipeTransferReq(req);

      Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
      Assert.assertEquals(req.getType(), deserializeReq.getType());
      Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

      Statement statement =
          req.constructStatement(); // will call PipeTransferTabletRawReq.sortTablet() here
      List<PartialPath> paths = new ArrayList<>();
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s1"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s2"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s3"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s4"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s5"}));
      paths.add(new PartialPath(new String[] {"root", "sg", "d", "s6"}));
      Assert.assertEquals(statement.getPaths(), paths);
    } catch (IOException e) {
      Assert.fail();
    }
  }

  @Test
  public void testPipeTransferTsFilePieceReq() throws IOException {
    byte[] body = "testPipeTransferTsFilePieceReq".getBytes();
    String fileName = "1.tsfile";

    PipeTransferTsFilePieceReq req =
        PipeTransferTsFilePieceReq.toTPipeTransferReq(fileName, 0, body);
    PipeTransferTsFilePieceReq deserializeReq =
        PipeTransferTsFilePieceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferSchemaSnapshotPieceReq() throws IOException {
    byte[] body = "testPipeTransferSchemaSnapshotPieceReq".getBytes();
    String fileName = "1.temp";

    PipeTransferSchemaSnapshotPieceReq req =
        PipeTransferSchemaSnapshotPieceReq.toTPipeTransferReq(fileName, 0, body);
    PipeTransferSchemaSnapshotPieceReq deserializeReq =
        PipeTransferSchemaSnapshotPieceReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getStartWritingOffset(), deserializeReq.getStartWritingOffset());
    Assert.assertArrayEquals(req.getFilePiece(), deserializeReq.getFilePiece());
  }

  @Test
  public void testPipeTransferTsFileSealReq() throws IOException {
    String fileName = "1.tsfile";

    PipeTransferTsFileSealReq req = PipeTransferTsFileSealReq.toTPipeTransferReq(fileName, 100);
    PipeTransferTsFileSealReq deserializeReq = PipeTransferTsFileSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getFileLength(), deserializeReq.getFileLength());
  }

  @Test
  public void testPipeTransferSchemaSnapshotSealReq() throws IOException {
    String fileName = "1.temp";

    PipeTransferSchemaSnapshotSealReq req =
        PipeTransferSchemaSnapshotSealReq.toTPipeTransferReq(fileName, 100);
    PipeTransferSchemaSnapshotSealReq deserializeReq =
        PipeTransferSchemaSnapshotSealReq.fromTPipeTransferReq(req);

    Assert.assertEquals(req.getVersion(), deserializeReq.getVersion());
    Assert.assertEquals(req.getType(), deserializeReq.getType());
    Assert.assertArrayEquals(req.getBody(), deserializeReq.getBody());

    Assert.assertEquals(req.getFileName(), deserializeReq.getFileName());
    Assert.assertEquals(req.getFileLength(), deserializeReq.getFileLength());
  }

  @Test
  public void testPIpeTransferFilePieceResp() throws IOException {
    PipeTransferFilePieceResp resp =
        PipeTransferFilePieceResp.toTPipeTransferResp(RpcUtils.SUCCESS_STATUS, 100);
    PipeTransferFilePieceResp deserializeResp =
        PipeTransferFilePieceResp.fromTPipeTransferResp(resp);

    Assert.assertEquals(resp.getStatus(), deserializeResp.getStatus());
    Assert.assertEquals(resp.getEndWritingOffset(), deserializeResp.getEndWritingOffset());
  }
}
