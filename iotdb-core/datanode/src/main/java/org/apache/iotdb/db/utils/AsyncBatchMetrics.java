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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncBatchMetrics implements IMetricSet {

  private static final Logger logger = LoggerFactory.getLogger(AsyncBatchMetrics.class);

  private volatile AbstractMetricService metricService;

  /**
   * The number of bytes occupied by data that has not yet been flushed in the current async batch
   * buffer queue.
   */
  private static final String ASYNC_BATCH_QUEUE_BYTES = "async_batch_queue_bytes";

  /** The total number of bytes flushed by the async batch so far, whether successfully or not. */
  private static final String ASYNC_BATCH_FLUSHED_BYTES_TOTAL = "async_batch_flushed_bytes_total";

  /** The latency in milliseconds for each flush operation of the async batch. */
  private static final String ASYNC_BATCH_FLUSH_LATENCY_MS = "async_batch_flush_latency_ms";

  /** The total number of failed flush entries in the async batch. */
  private static final String ASYNC_BATCH_FLUSH_FAILED_NUM_TOTAL =
      "async_batch_flush_failed_num_total";

  /** The total number of failed flush bytes in the async batch. */
  private static final String ASYNC_BATCH_FLUSH_FAILED_BYTES_TOTAL =
      "async_batch_flush_failed_bytes_total";

  /** The number of entries flushed in each flush operation of the async batch. */
  private static final String ASYNC_BATCH_FLUSH_NUM = "async_batch_flush_num";

  /** Total bytes flushed in each flush operation of the async batch. */
  private static final String ASYNC_BATCH_FLUSH_BYTES = "async_batch_flush_bytes";

  private final Set<String> asyncBatchUtilsNames = new HashSet<>();

  private final Map<String, Gauge> asyncBatchQueueBytesMap = new ConcurrentHashMap<>();
  private final Map<String, Counter> asyncBatchFlushedBytesTotalMap = new ConcurrentHashMap<>();
  private final Map<String, Histogram> asyncBatchFlushLatencyMsMap = new ConcurrentHashMap<>();
  private final Map<String, Counter> asyncBatchFlushFailedNumTotalMap = new ConcurrentHashMap<>();
  private final Map<String, Counter> asyncBatchFlushFailedBytesTotalMap = new ConcurrentHashMap<>();
  private final Map<String, Histogram> asyncBatchFlushNumMap = new ConcurrentHashMap<>();
  private final Map<String, Histogram> asyncBatchFlushBytesMap = new ConcurrentHashMap<>();

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    final ImmutableSet<String> asyncBatchNames = ImmutableSet.copyOf(asyncBatchUtilsNames);
    for (String asyncBatchName : asyncBatchNames) {
      register(asyncBatchName);
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    final ImmutableSet<String> asyncBatchNames = ImmutableSet.copyOf(asyncBatchUtilsNames);
    for (String asyncBatchName : asyncBatchNames) {
      deregister(asyncBatchName);
    }
    if (!asyncBatchUtilsNames.isEmpty()) {
      logger.warn(
          "Failed to unbind from metric service, some async batch metrics are still registered");
    }
  }

  public void register(String asyncBatchName) {
    asyncBatchUtilsNames.add(asyncBatchName);
    if (Objects.nonNull(metricService)) {
      // if metric service is already bound, create metrics immediately
      createMetrics(asyncBatchName);
    }
  }

  public void createMetrics(String asyncBatchName) {
    // create metrics
    asyncBatchQueueBytesMap.putIfAbsent(
        asyncBatchName,
        metricService.getOrCreateGauge(
            ASYNC_BATCH_QUEUE_BYTES, MetricLevel.IMPORTANT, Tag.NAME.toString(), asyncBatchName));
    asyncBatchFlushedBytesTotalMap.putIfAbsent(
        asyncBatchName,
        metricService.getOrCreateCounter(
            ASYNC_BATCH_FLUSHED_BYTES_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            asyncBatchName));
    asyncBatchFlushLatencyMsMap.putIfAbsent(
        asyncBatchName,
        metricService.getOrCreateHistogram(
            ASYNC_BATCH_FLUSH_LATENCY_MS,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            asyncBatchName));
    asyncBatchFlushFailedNumTotalMap.putIfAbsent(
        asyncBatchName,
        metricService.getOrCreateCounter(
            ASYNC_BATCH_FLUSH_FAILED_NUM_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            asyncBatchName));
    asyncBatchFlushFailedBytesTotalMap.putIfAbsent(
        asyncBatchName,
        metricService.getOrCreateCounter(
            ASYNC_BATCH_FLUSH_FAILED_BYTES_TOTAL,
            MetricLevel.IMPORTANT,
            Tag.NAME.toString(),
            asyncBatchName));
    asyncBatchFlushNumMap.putIfAbsent(
        asyncBatchName,
        metricService.getOrCreateHistogram(
            ASYNC_BATCH_FLUSH_NUM, MetricLevel.IMPORTANT, Tag.NAME.toString(), asyncBatchName));
    asyncBatchFlushBytesMap.putIfAbsent(
        asyncBatchName,
        metricService.getOrCreateHistogram(
            ASYNC_BATCH_FLUSH_BYTES, MetricLevel.IMPORTANT, Tag.NAME.toString(), asyncBatchName));
  }

  public void deregister(String asyncBatchName) {
    if (!asyncBatchUtilsNames.contains(asyncBatchName)) {
      logger.warn("Async batch metrics {} not registered", asyncBatchName);
      return;
    }
    if (Objects.nonNull(metricService)) {
      removeMetrics(asyncBatchName);
    }
    asyncBatchUtilsNames.remove(asyncBatchName);
  }

  public void removeMetrics(String asyncBatchName) {
    metricService.remove(
        MetricType.GAUGE, ASYNC_BATCH_QUEUE_BYTES, Tag.NAME.toString(), asyncBatchName);
    metricService.remove(
        MetricType.COUNTER, ASYNC_BATCH_FLUSHED_BYTES_TOTAL, Tag.NAME.toString(), asyncBatchName);
    metricService.remove(
        MetricType.HISTOGRAM, ASYNC_BATCH_FLUSH_LATENCY_MS, Tag.NAME.toString(), asyncBatchName);
    metricService.remove(
        MetricType.COUNTER,
        ASYNC_BATCH_FLUSH_FAILED_NUM_TOTAL,
        Tag.NAME.toString(),
        asyncBatchName);
    metricService.remove(
        MetricType.COUNTER,
        ASYNC_BATCH_FLUSH_FAILED_BYTES_TOTAL,
        Tag.NAME.toString(),
        asyncBatchName);
    metricService.remove(
        MetricType.HISTOGRAM, ASYNC_BATCH_FLUSH_NUM, Tag.NAME.toString(), asyncBatchName);
    metricService.remove(
        MetricType.HISTOGRAM, ASYNC_BATCH_FLUSH_BYTES, Tag.NAME.toString(), asyncBatchName);
    asyncBatchQueueBytesMap.remove(asyncBatchName);
    asyncBatchFlushedBytesTotalMap.remove(asyncBatchName);
    asyncBatchFlushLatencyMsMap.remove(asyncBatchName);
    asyncBatchFlushFailedNumTotalMap.remove(asyncBatchName);
    asyncBatchFlushFailedBytesTotalMap.remove(asyncBatchName);
    asyncBatchFlushNumMap.remove(asyncBatchName);
    asyncBatchFlushBytesMap.remove(asyncBatchName);
  }

  // update methods for metrics
  public void updateAsyncBatchQueueBytes(final String asyncBatchName, final long bytes) {
    Gauge gauge = asyncBatchQueueBytesMap.get(asyncBatchName);
    if (gauge == null) {
      logger.warn(
          "Failed to set async batch queue bytes for {}, metric not registered", asyncBatchName);
      return;
    }
    gauge.set(bytes);
  }

  public void incAsyncBatchFlushedBytesTotal(final String asyncBatchName, final long bytes) {
    Counter counter = asyncBatchFlushedBytesTotalMap.get(asyncBatchName);
    if (counter == null) {
      logger.warn(
          "Failed to increment async batch flushed bytes total for {}, metric not registered",
          asyncBatchName);
      return;
    }
    counter.inc(bytes);
  }

  public void recordAsyncBatchFlushLatencyMs(final String asyncBatchName, final long latencyMs) {
    Histogram histogram = asyncBatchFlushLatencyMsMap.get(asyncBatchName);
    if (histogram == null) {
      logger.warn(
          "Failed to record async batch flush latency ms for {}, metric not registered",
          asyncBatchName);
      return;
    }
    histogram.update(latencyMs);
  }

  public void incAsyncBatchFlushFailedNumTotal(final String asyncBatchName, final long num) {
    Counter counter = asyncBatchFlushFailedNumTotalMap.get(asyncBatchName);
    if (counter == null) {
      logger.warn(
          "Failed to increment async batch flush failed num total for {}, metric not registered",
          asyncBatchName);
      return;
    }
    counter.inc(num);
  }

  public void incAsyncBatchFlushFailedBytesTotal(final String asyncBatchName, final long bytes) {
    Counter counter = asyncBatchFlushFailedBytesTotalMap.get(asyncBatchName);
    if (counter == null) {
      logger.warn(
          "Failed to increment async batch flush failed bytes total for {}, metric not registered",
          asyncBatchName);
      return;
    }
    counter.inc(bytes);
  }

  public void recordAsyncBatchFlushNum(final String asyncBatchName, final long num) {
    Histogram histogram = asyncBatchFlushNumMap.get(asyncBatchName);
    if (histogram == null) {
      logger.warn(
          "Failed to record async batch flush num for {}, metric not registered", asyncBatchName);
      return;
    }
    histogram.update(num);
  }

  public void recordAsyncBatchFlushBytes(final String asyncBatchName, final long bytes) {
    Histogram histogram = asyncBatchFlushBytesMap.get(asyncBatchName);
    if (histogram == null) {
      logger.warn(
          "Failed to record async batch flush bytes for {}, metric not registered", asyncBatchName);
      return;
    }
    histogram.update(bytes);
  }

  //////////////////////////// singleton ////////////////////////////
  public static AsyncBatchMetrics getInstance() {
    return AsyncBatchMetricsHolder.INSTANCE;
  }

  private static class AsyncBatchMetricsHolder {
    private static final AsyncBatchMetrics INSTANCE = new AsyncBatchMetrics();

    // empty constructor
    private AsyncBatchMetricsHolder() {}
  }

  private AsyncBatchMetrics() {}
}
