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

import org.apache.iotdb.confignode.procedure.impl.schema.table.SetTablePropertiesProcedure;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class SetViewPropertiesProcedure extends SetTablePropertiesProcedure {
  public SetViewPropertiesProcedure(final boolean isGeneratedByPipe) {
    super(isGeneratedByPipe);
  }

  public SetViewPropertiesProcedure(
      final String database,
      final String tableName,
      final String queryId,
      final Map<String, String> properties,
      final boolean isGeneratedByPipe) {
    super(database, tableName, queryId, properties, isGeneratedByPipe);
  }

  @Override
  protected String getActionMessage() {
    return "set view properties";
  }

  @Override
  public void serialize(final DataOutputStream stream) throws IOException {
    stream.writeShort(
        isGeneratedByPipe
            ? ProcedureType.PIPE_ENRICHED_SET_VIEW_PROPERTIES_PROCEDURE.getTypeCode()
            : ProcedureType.SET_VIEW_PROPERTIES_PROCEDURE.getTypeCode());
    innerSerialize(stream);
  }
}
