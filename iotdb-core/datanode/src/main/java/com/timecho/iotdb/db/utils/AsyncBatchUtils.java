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
package com.timecho.iotdb.db.utils;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Accountable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AsyncBatchUtils
 *
 * <p>A generic asynchronous batching utility.
 *
 * <p>Features: - Producer/consumer model based on BlockingQueue - Periodic flush and
 * threshold-triggered flush - Flush drains all buffered data at once - Internal retry mechanism on
 * flush failure (no re-queue) - Ensures only one flush task runs at a time
 */
public class AsyncBatchUtils<T extends Accountable> {
  private static final Logger logger = LoggerFactory.getLogger(AsyncBatchUtils.class);

  /* ================= Configuration ================= */

  /** Maximum retry count when flush fails */
  private static final int INSERT_RETRY_COUNT = 5;

  /** Retry interval (milliseconds) when flush fails */
  private static final int INSERT_RETRY_INTERVAL_MS = 100;

  /* ================= Internal Components ================= */

  /** Buffer queue for incoming data, maybe need to add maxQueueSize */
  private final BlockingQueue<BatchItem<T>> queue;

  /** Flush mutex to guarantee single flush execution */
  private final AtomicBoolean flushing = new AtomicBoolean(false);

  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final AtomicLong currentQueueBytes = new AtomicLong(0);

  /** Executor responsible for executing flush logic */
  private final ExecutorService flushExecutor;

  /** Scheduler used only for triggering flush */
  private final ScheduledExecutorService scheduler;

  /** Periodic flush interval (milliseconds) */
  private final long flushIntervalMs;

  /** Consumer that handles the actual batch processing */
  private final BatchConsumer<T> consumer;

  private final String name;

  private long maxQueueBytes = 256L * 1024L * 1024L; // 256MB

  private long flushWatermarkBytes = maxQueueBytes * 8L / 10L; // 204MB

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition notEnoughMemory = lock.newCondition();

  /* ================= Lifecycle ================= */

  public AsyncBatchUtils(
      String name, long flushIntervalMs, long maxQueueBytes, BatchConsumer<T> consumer) {
    this.name = name;
    this.flushIntervalMs = flushIntervalMs;
    this.maxQueueBytes = maxQueueBytes;
    this.flushWatermarkBytes = maxQueueBytes * 8L / 10L;
    this.consumer = consumer;
    this.flushExecutor =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(
            ThreadName.ASYNC_BATCH_WRITE_DATA.getName() + "-" + name);
    this.scheduler =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.ASYNC_BATCH_WRITE_SCHEDULER_IN_TIME.getName() + "-" + name);
    this.queue = new LinkedBlockingQueue<>();
  }

  /** Start the periodic flush scheduler */
  public void start() {
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduler,
        this::triggerFlushAsync,
        flushIntervalMs,
        flushIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  /* ================= Public API ================= */

  /** Push a single data item into the buffer */
  public CompletableFuture<TSStatus> push(T data) throws InterruptedException {
    if (closed.get()) {
      CompletableFuture<TSStatus> f = new CompletableFuture<>();
      f.completeExceptionally(
          new IllegalStateException(String.format("[AsyncBatchUtils] %s already shutdown", name)));
      return f;
    }

    BatchItem<T> item = new BatchItem<>(data);
    long bytes = data.ramBytesUsed();

    lock.lockInterruptibly();
    try {
      while (currentQueueBytes.get() + bytes > maxQueueBytes) {
        notEnoughMemory.await();
      }

      long afterAdd = currentQueueBytes.addAndGet(bytes);
      queue.offer(item);

      if (afterAdd >= flushWatermarkBytes) {
        triggerFlushAsync();
      }

      return item.future;
    } finally {
      lock.unlock();
    }
  }

  /** Trigger an asynchronous flush with mutual exclusion */
  private void triggerFlushAsync() {
    if (queue.isEmpty()) {
      return;
    }

    if (flushing.compareAndSet(false, true)) {
      flushExecutor.submit(
          () -> {
            try {
              flushInternal();
            } finally {
              flushing.set(false);
            }
          });
    }
  }

  /** Flush implementation: drain all data and retry on failure */
  private void flushInternal() {
    List<BatchItem<T>> items = new ArrayList<>();
    queue.drainTo(items);

    if (items.isEmpty()) {
      return;
    }

    long releasedBytes = 0;
    List<T> batch = new ArrayList<>(items.size());
    for (BatchItem<T> item : items) {
      batch.add(item.data);
      releasedBytes += item.data.ramBytesUsed();
    }

    try {
      ExecutionResult insertResult = null;
      for (int retry = 0; retry < INSERT_RETRY_COUNT; retry++) {
        insertResult = consumer.consume(batch);
        if (insertResult.status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          for (BatchItem<T> item : items) {
            item.future.complete(insertResult.status);
          }
          return;
        }
        if (insertResult.status.isNeedRetry()) {
          TimeUnit.MILLISECONDS.sleep(INSERT_RETRY_INTERVAL_MS);
        } else {
          break;
        }
      }
      Exception e = new IoTDBRuntimeException(insertResult.status);
      handlePermanentFailure(items, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      handlePermanentFailure(items, e);
    } catch (Exception e) {
      handlePermanentFailure(items, e);
    } finally {
      currentQueueBytes.addAndGet(-releasedBytes);
      notEnoughMemory.signalAll();
    }
  }

  /** Handle permanently failed batches */
  private void handlePermanentFailure(List<BatchItem<T>> items, Exception e) {

    logger.warn("[AsyncBatchUtils] {} Failed to write because", name, e);

    for (BatchItem<T> item : items) {
      item.future.completeExceptionally(e);
    }
  }

  /** Gracefully shut down all background threads */
  public void shutdown() {
    if (closed.get()) {
      return;
    }
    // Stop the scheduled flush task
    if (scheduler != null) {
      scheduler.shutdownNow();
      try {
        if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
          logger.warn(
              "[AsyncBatchUtils] {} Scheduler did not terminate within 5 seconds, forcing shutdown",
              name);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("[AsyncBatchUtils] {} Interrupted while shutting down scheduler", name, e);
      }
    }

    // Stop the flush executor
    if (flushExecutor != null) {
      flushExecutor.shutdown(); // Stop accepting new tasks
      try {
        // Wait for the current flush task to complete
        if (!flushExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
          logger.warn(
              "[AsyncBatchUtils] {} FlushExecutor did not terminate within 10 seconds, forcing shutdownNow",
              name);
          flushExecutor.shutdownNow();
        }
      } catch (InterruptedException e) {
        flushExecutor.shutdownNow();
        Thread.currentThread().interrupt();
        logger.warn("[AsyncBatchUtils] {} Interrupted while shutting down flushExecutor", name, e);
      }
    }
    closed.set(true);

    logger.info("[AsyncBatchUtils] {} Shutdown completed", name);
  }

  private static class BatchItem<T> {
    final T data;
    final CompletableFuture<TSStatus> future;

    BatchItem(T data) {
      this.data = data;
      this.future = new CompletableFuture<>();
    }
  }

  /* ================= Callback Interface ================= */

  @FunctionalInterface
  public interface BatchConsumer<T> {
    ExecutionResult consume(List<T> batch) throws Exception;
  }
}
