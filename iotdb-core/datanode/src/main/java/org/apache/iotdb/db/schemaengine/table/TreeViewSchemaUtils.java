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

package org.apache.iotdb.db.schemaengine.table;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.iotdb.commons.schema.table.TreeViewSchema.TREE_VIEW_DATABASE;

public class TreeViewSchemaUtils {

  public static void checkDBNameInWrite(final String dbName) {
    if (TreeViewSchema.isTreeViewDatabase(dbName)) {
      throw new SemanticException(
          new IoTDBException(
              "The database 'tree_view_db' only accepts rename table and rename column for write requests",
              TSStatusCode.SEMANTIC_ERROR.getStatusCode()));
    }
  }

  public static void checkDBNameInRename(final String dbName) {
    if (!TreeViewSchema.isTreeViewDatabase(dbName)) {
      throw new SemanticException(
          new IoTDBException(
              "Renaming table and column only supports database 'tree_view_db'",
              TSStatusCode.SEMANTIC_ERROR.getStatusCode()));
    }
  }

  public static void buildDatabaseTsBlock(final TsBlockBuilder builder, final boolean details) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder
        .getColumnBuilder(0)
        .writeBinary(new Binary(TREE_VIEW_DATABASE, TSFileConfig.STRING_CHARSET));
    builder.getColumnBuilder(1).appendNull();

    builder.getColumnBuilder(2).appendNull();
    builder.getColumnBuilder(3).appendNull();
    builder.getColumnBuilder(4).appendNull();
    if (details) {
      builder.getColumnBuilder(5).appendNull();
      builder.getColumnBuilder(6).appendNull();
    }
    builder.declarePosition();
  }

  public static String getOriginalDatabase(final TsTable table) {
    return table
        .getPropValue(TreeViewSchema.TREE_DATABASE)
        .orElseThrow(
            () ->
                new SemanticException(
                    String.format(
                        "Failed to get the original database, because the %s is null for table %s",
                        TreeViewSchema.TREE_DATABASE, table.getTableName())));
  }

  public static IDeviceID convertToIDeviceID(final String database, final String[] idValues) {
    return IDeviceID.Factory.DEFAULT_FACTORY.create(
        StringArrayDeviceID.splitDeviceIdString(
            Stream.concat(
                    Arrays.stream(forceSeparateStringToPartialPathNodes(database)),
                    Arrays.stream(idValues))
                .toArray(String[]::new)));
  }

  public static String[] forceSeparateStringToPartialPathNodes(final String string) {
    final String[] databaseNodes;
    try {
      databaseNodes = new PartialPath(string).getNodes();
    } catch (final IllegalPathException e) {
      throw new SemanticException(
          String.format(
              "Failed to parse the tree view string %s when convert to IDeviceID", string));
    }
    return databaseNodes;
  }

  private TreeViewSchemaUtils() {
    // Private constructor
  }
}
