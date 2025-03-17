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

package org.apache.iotdb.confignode.procedure.impl.schema.table.view;

import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.confignode.procedure.impl.schema.table.AddTableColumnProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class AddTableViewColumnProcedure extends AddTableColumnProcedure {
  public AddTableViewColumnProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public AddTableViewColumnProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final List<TsTableColumnSchema> addedColumnList,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, addedColumnList, isGeneratedByPipe);
  }

  @Override
  protected String getActionMessage() {
    return "add view column";
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_ADD_TABLE_VIEW_COLUMN_PROCEDURE.getTypeCode()
            : ProcedureType.ADD_TABLE_VIEW_COLUMN_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }
}
