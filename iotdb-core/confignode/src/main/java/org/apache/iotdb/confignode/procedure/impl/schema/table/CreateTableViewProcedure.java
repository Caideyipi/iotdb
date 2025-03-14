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

package org.apache.iotdb.confignode.procedure.impl.schema.table;

import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CreateTableViewProcedure extends CreateTableProcedure {

  private boolean replace;

  public CreateTableViewProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public CreateTableViewProcedure(
      final String database,
      final TsTable table,
      final boolean replace,
      final boolean isGeneratedByPipe) {
    super(database, table, isGeneratedByPipe);
    this.replace = replace;
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_CREATE_TABLE_VIEW_PROCEDURE.getTypeCode()
            : ProcedureType.CREATE_TABLE_VIEW_PROCEDURE.getTypeCode());
    serializeAttributes(stream);
    ReadWriteIOUtils.write(replace, stream);
  }

  @Override
  public void deserialize(final ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    replace = ReadWriteIOUtils.readBool(byteBuffer);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o) && replace == ((CreateTableViewProcedure) o).replace;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), replace);
  }
}
