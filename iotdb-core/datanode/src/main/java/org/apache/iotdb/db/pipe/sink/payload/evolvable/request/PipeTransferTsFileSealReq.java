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

package org.apache.iotdb.db.pipe.sink.payload.evolvable.request;

import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeRequestType;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferFileSealReqV1;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import java.io.IOException;

public class PipeTransferTsFileSealReq extends PipeTransferFileSealReqV1 {

  private PipeTransferTsFileSealReq() {
    // Empty constructor
  }

  @Override
  protected PipeRequestType getPlanType() {
    return PipeRequestType.TRANSFER_TS_FILE_SEAL;
  }

  /////////////////////////////// Thrift ///////////////////////////////

  public static PipeTransferTsFileSealReq toTPipeTransferReq(String fileName, long fileLength)
      throws IOException {
    return (PipeTransferTsFileSealReq)
        new PipeTransferTsFileSealReq().convertToTPipeTransferReq(fileName, fileLength);
  }

  public static PipeTransferTsFileSealReq fromTPipeTransferReq(TPipeTransferReq req) {
    return (PipeTransferTsFileSealReq)
        new PipeTransferTsFileSealReq().translateFromTPipeTransferReq(req);
  }

  /////////////////////////////// Air Gap ///////////////////////////////

  public static byte[] toTPipeTransferBytes(String fileName, long fileLength) throws IOException {
    return new PipeTransferTsFileSealReq()
        .convertToTPipeTransferSnapshotSealBytes(fileName, fileLength);
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(Object obj) {
    return obj instanceof PipeTransferTsFileSealReq && super.equals(obj);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
