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

package org.apache.iotdb.commons.schema.table;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.schema.table.column.AttributeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TimeColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class TsTable {

  public static final String TIME_COLUMN_NAME = "time";
  private static final TimeColumnSchema TIME_COLUMN_SCHEMA =
      new TimeColumnSchema(TIME_COLUMN_NAME, TSDataType.TIMESTAMP);

  public static final Map<String, Object> TABLE_ALLOWED_PROPERTIES_2_DEFAULT_VALUE_MAP =
      new HashMap<>();

  public static final String TTL_PROPERTY = "TTL";

  static {
    TABLE_ALLOWED_PROPERTIES_2_DEFAULT_VALUE_MAP.put(
        TTL_PROPERTY.toLowerCase(Locale.ENGLISH), new Binary("INF", TSFileConfig.STRING_CHARSET));
  }

  private final String tableName;

  private final Map<String, TsTableColumnSchema> columnSchemaMap = new LinkedHashMap<>();

  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private Map<String, String> props = null;

  // We intern the attributeIds to speed up the "dropped column check" in attribute names, and
  // decouple from java's internal Integer pool. This does not contain any extra information and
  // shall only be used in table cache.
  private final Map<Integer, Integer> attributeIdPool = new HashMap<>();

  private transient int idNum = 0;
  private int attributeNum = 0;

  public TsTable(final String tableName) {
    this.tableName = tableName;
    columnSchemaMap.put(TIME_COLUMN_NAME, TIME_COLUMN_SCHEMA);
  }

  public String getTableName() {
    return tableName;
  }

  public TsTableColumnSchema getColumnSchema(final String columnName) {
    readWriteLock.readLock().lock();
    try {
      return columnSchemaMap.get(columnName);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void addColumnSchema(final TsTableColumnSchema columnSchema) {
    readWriteLock.writeLock().lock();
    try {
      columnSchemaMap.put(columnSchema.getColumnName(), columnSchema);
      if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
        idNum++;
      } else if (columnSchema.getColumnCategory().equals(TsTableColumnCategory.ATTRIBUTE)) {
        ((AttributeColumnSchema) columnSchema).setId(attributeNum++);
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  // Currently only supports attribute column
  public void renameColumnSchema(final String oldName, final String newName) {
    readWriteLock.writeLock().lock();
    try {
      // Ensures idempotency
      if (columnSchemaMap.containsKey(oldName)) {
        final AttributeColumnSchema schema =
            (AttributeColumnSchema) columnSchemaMap.remove(oldName);
        columnSchemaMap.put(
            newName,
            new AttributeColumnSchema(
                newName, schema.getDataType(), schema.getProps(), schema.getId()));
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void removeColumnSchema(final String columnName) {
    readWriteLock.writeLock().lock();
    try {
      final TsTableColumnSchema columnSchema = columnSchemaMap.remove(columnName);
      if (columnSchema != null
          && columnSchema.getColumnCategory().equals(TsTableColumnCategory.ID)) {
        idNum--;
      }
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public int getColumnNum() {
    readWriteLock.readLock().lock();
    try {
      return columnSchemaMap.size();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public int getIdNum() {
    readWriteLock.readLock().lock();
    try {
      return idNum;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public List<TsTableColumnSchema> getColumnList() {
    readWriteLock.readLock().lock();
    try {
      return new ArrayList<>(columnSchemaMap.values());
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public long getTableTTL() {
    long ttl = getTableTTLInMS();
    return ttl == Long.MAX_VALUE
        ? ttl
        : CommonDateTimeUtils.convertMilliTimeWithPrecision(
            ttl, CommonDescriptor.getInstance().getConfig().getTimestampPrecision());
  }

  public long getTableTTLInMS() {
    return Long.parseLong(
        getPropValue(TTL_PROPERTY.toLowerCase(Locale.ENGLISH)).orElse(Long.MAX_VALUE + ""));
  }

  public Optional<String> getPropValue(final String propKey) {
    readWriteLock.readLock().lock();
    try {
      return props != null && props.containsKey(propKey)
          ? Optional.of(props.get(propKey))
          : Optional.empty();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void addProp(final String key, final String value) {
    readWriteLock.writeLock().lock();
    try {
      if (props == null) {
        props = new HashMap<>();
      }
      props.put(key, value);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public void removeProp(final String key) {
    readWriteLock.writeLock().lock();
    try {
      if (props == null) {
        return;
      }
      props.remove(key);
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  public int getAttributeId(final String name) {
    readWriteLock.readLock().lock();
    try {
      return ((AttributeColumnSchema) columnSchemaMap.get(name)).getId();
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public String getAttributeName(final int id) {
    readWriteLock.readLock().lock();
    try {
      for (final TsTableColumnSchema schema : columnSchemaMap.values()) {
        if (schema.getColumnCategory() == TsTableColumnCategory.ATTRIBUTE
            && id == (((AttributeColumnSchema) schema).getId())) {
          return schema.getColumnName();
        }
      }
      return null;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public Integer getInternAttributeId(final @Nonnull Integer attributeId) {
    readWriteLock.readLock().lock();
    try {
      if (attributeIdPool.containsKey(attributeId)) {
        return attributeIdPool.get(attributeId);
      }
      for (final TsTableColumnSchema schema : columnSchemaMap.values()) {
        if (schema.getColumnCategory() == TsTableColumnCategory.ATTRIBUTE
            && attributeId.equals(((AttributeColumnSchema) schema).getId())) {
          final Integer internId = ((AttributeColumnSchema) schema).getId();
          attributeIdPool.put(internId, internId);
          return internId;
        }
      }
      return null;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public byte[] serialize() {
    final ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      serialize(stream);
    } catch (final IOException ignored) {
      // won't happen
    }
    return stream.toByteArray();
  }

  public void serialize(final OutputStream stream) throws IOException {
    ReadWriteIOUtils.write(tableName, stream);
    ReadWriteIOUtils.write(columnSchemaMap.size(), stream);
    for (final TsTableColumnSchema columnSchema : columnSchemaMap.values()) {
      TsTableColumnSchemaUtil.serialize(columnSchema, stream);
    }
    ReadWriteIOUtils.write(props, stream);
    ReadWriteIOUtils.write(attributeNum, stream);
  }

  public static TsTable deserialize(final InputStream inputStream) throws IOException {
    final String name = ReadWriteIOUtils.readString(inputStream);
    final TsTable table = new TsTable(name);
    final int columnNum = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < columnNum; i++) {
      table.addColumnSchema(TsTableColumnSchemaUtil.deserialize(inputStream));
    }
    table.props = ReadWriteIOUtils.readMap(inputStream);
    table.attributeNum = ReadWriteIOUtils.readInt(inputStream);
    return table;
  }

  public static TsTable deserialize(final ByteBuffer buffer) {
    final String name = ReadWriteIOUtils.readString(buffer);
    final TsTable table = new TsTable(name);
    final int columnNum = ReadWriteIOUtils.readInt(buffer);
    for (int i = 0; i < columnNum; i++) {
      table.addColumnSchema(TsTableColumnSchemaUtil.deserialize(buffer));
    }
    table.props = ReadWriteIOUtils.readMap(buffer);
    table.attributeNum = ReadWriteIOUtils.readInt(buffer);
    return table;
  }

  public void setProps(final Map<String, String> props) {
    readWriteLock.writeLock().lock();
    try {
      this.props = props;
    } finally {
      readWriteLock.writeLock().unlock();
    }
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName);
  }

  @Override
  public String toString() {
    return "TsTable{"
        + "tableName='"
        + tableName
        + '\''
        + ", columnSchemaMap="
        + columnSchemaMap
        + ", props="
        + props
        + '}';
  }
}
