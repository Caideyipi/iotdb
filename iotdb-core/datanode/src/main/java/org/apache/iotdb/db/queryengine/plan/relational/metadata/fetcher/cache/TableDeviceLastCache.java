/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache;

import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.lastcache.LastCacheContainer;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.TsPrimitiveType;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@ThreadSafe
public class TableDeviceLastCache {
  static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(TableDeviceLastCache.class);

  private final Map<String, TimeValuePair> measurement2CachedLastMap = new ConcurrentHashMap<>();
  private long lastTime = Long.MIN_VALUE;

  public int update(
      final @Nonnull String database,
      final @Nonnull String tableName,
      final Map<String, TimeValuePair> measurementUpdateMap) {
    final AtomicInteger diff = new AtomicInteger(0);
    measurementUpdateMap.forEach(
        (k, v) -> {
          if (!measurement2CachedLastMap.containsKey(k)) {
            k = DataNodeTableCache.getInstance().tryGetInternColumnName(database, tableName, k);
          }
          if (lastTime < v.getTimestamp()) {
            lastTime = v.getTimestamp();
          }
          measurement2CachedLastMap.compute(
              k,
              (measurement, tvPair) -> {
                if (Objects.isNull(tvPair) || tvPair.getTimestamp() <= v.getTimestamp()) {
                  diff.addAndGet(
                      Objects.nonNull(tvPair)
                          ? LastCacheContainer.getDiffSize(tvPair.getValue(), v.getValue())
                          : (int) RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY + v.getSize());
                  return v;
                }
                return tvPair;
              });
        });
    return diff.get();
  }

  public TimeValuePair getTimeValuePair(final @Nonnull String measurement) {
    return measurement2CachedLastMap.get(measurement);
  }

  // Shall pass in "" if last by time
  public Long getLastTime(final @Nonnull String measurement) {
    if (isAllNull(measurement)) {
      return null;
    }
    return getAlignTime(measurement);
  }

  // Shall pass in "" if last by time
  public TsPrimitiveType getLastBy(
      final @Nonnull String measurement, final @Nonnull String targetMeasurement) {
    if (isAllNull(measurement)) {
      return null;
    }
    final TimeValuePair tvPair = measurement2CachedLastMap.get(targetMeasurement);
    return Objects.nonNull(tvPair) && tvPair.getTimestamp() == getAlignTime(measurement)
        ? tvPair.getValue()
        : null;
  }

  // Shall pass in "" if last by time
  public Pair<Long, Map<String, TsPrimitiveType>> getLastRow(final @Nonnull String measurement) {
    if (isAllNull(measurement)) {
      return null;
    }
    final long alignTime = getAlignTime(measurement);
    return new Pair<>(
        alignTime,
        measurement2CachedLastMap.entrySet().stream()
            .filter(entry -> entry.getValue().getTimestamp() == alignTime)
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getValue())));
  }

  private boolean isAllNull(final @Nonnull String measurement) {
    return !Objects.equals(measurement, "") && !measurement2CachedLastMap.containsKey(measurement);
  }

  private long getAlignTime(final @Nonnull String measurement) {
    return !Objects.equals(measurement, "")
        ? measurement2CachedLastMap.get(measurement).getTimestamp()
        : lastTime;
  }

  public int estimateSize() {
    return INSTANCE_SIZE
        + (int) RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY * measurement2CachedLastMap.size()
        + measurement2CachedLastMap.values().stream()
            .mapToInt(TimeValuePair::getSize)
            .reduce(0, Integer::sum);
  }
}
