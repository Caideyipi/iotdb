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

package org.apache.iotdb.commons.pipe.connector.payload.thrift.request;

import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

<<<<<<<< HEAD:iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/pipe/connector/payload/request/PipeTransferHandshakeReq.java
public abstract class PipeTransferHandshakeReq extends TPipeTransferReq {

  private transient String timestampPrecision;

  public final String getTimestampPrecision() {
========
public class PipeTransferHandshakeV1Req extends TPipeTransferReq {

  private transient String timestampPrecision;

  private PipeTransferHandshakeV1Req() {
    // Empty constructor
  }

  public String getTimestampPrecision() {
>>>>>>>> 6943524b000217bf6d4678b51097f93cfedad8f3:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/connector/payload/evolvable/request/PipeTransferHandshakeV1Req.java
    return timestampPrecision;
  }

  protected abstract PipeRequestType getPlanType();

  /////////////////////////////// Thrift ///////////////////////////////

<<<<<<<< HEAD:iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/pipe/connector/payload/request/PipeTransferHandshakeReq.java
  protected final PipeTransferHandshakeReq convertToTPipeTransferReq(String timestampPrecision)
      throws IOException {
    this.timestampPrecision = timestampPrecision;

    this.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    this.type = getPlanType().getType();
========
  public static PipeTransferHandshakeV1Req toTPipeTransferReq(String timestampPrecision)
      throws IOException {
    final PipeTransferHandshakeV1Req handshakeReq = new PipeTransferHandshakeV1Req();

    handshakeReq.timestampPrecision = timestampPrecision;

    handshakeReq.version = IoTDBConnectorRequestVersion.VERSION_1.getVersion();
    handshakeReq.type = PipeRequestType.DATANODE_HANDSHAKE_V1.getType();
>>>>>>>> 6943524b000217bf6d4678b51097f93cfedad8f3:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/connector/payload/evolvable/request/PipeTransferHandshakeV1Req.java
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(timestampPrecision, outputStream);
      this.body = ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }

    return this;
  }

<<<<<<<< HEAD:iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/pipe/connector/payload/request/PipeTransferHandshakeReq.java
  protected final PipeTransferHandshakeReq translateFromTPipeTransferReq(
      TPipeTransferReq transferReq) {
    timestampPrecision = ReadWriteIOUtils.readString(transferReq.body);
========
  public static PipeTransferHandshakeV1Req fromTPipeTransferReq(TPipeTransferReq transferReq) {
    final PipeTransferHandshakeV1Req handshakeReq = new PipeTransferHandshakeV1Req();
>>>>>>>> 6943524b000217bf6d4678b51097f93cfedad8f3:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/connector/payload/evolvable/request/PipeTransferHandshakeV1Req.java

    version = transferReq.version;
    type = transferReq.type;
    body = transferReq.body;

    return this;
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  protected final byte[] convertToTransferHandshakeBytes(String timestampPrecision)
      throws IOException {
    try (final PublicBAOS byteArrayOutputStream = new PublicBAOS();
        final DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      ReadWriteIOUtils.write(IoTDBConnectorRequestVersion.VERSION_1.getVersion(), outputStream);
<<<<<<<< HEAD:iotdb-core/node-commons/src/main/java/org/apache/iotdb/commons/pipe/connector/payload/request/PipeTransferHandshakeReq.java
      ReadWriteIOUtils.write(getPlanType().getType(), outputStream);
========
      ReadWriteIOUtils.write(PipeRequestType.DATANODE_HANDSHAKE_V1.getType(), outputStream);
>>>>>>>> 6943524b000217bf6d4678b51097f93cfedad8f3:iotdb-core/datanode/src/main/java/org/apache/iotdb/db/pipe/connector/payload/evolvable/request/PipeTransferHandshakeV1Req.java
      ReadWriteIOUtils.write(timestampPrecision, outputStream);
      return byteArrayOutputStream.toByteArray();
    }
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeTransferHandshakeV1Req that = (PipeTransferHandshakeV1Req) obj;
    return timestampPrecision.equals(that.timestampPrecision)
        && version == that.version
        && type == that.type
        && body.equals(that.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestampPrecision, version, type, body);
  }
}
