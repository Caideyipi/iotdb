package org.apache.iotdb.mqtt;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.metrics.type.Counter;
import org.apache.iotdb.metrics.type.Gauge;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.utils.MetricLevel;

import io.moquette.broker.config.IConfig;
import io.moquette.metrics.MetricsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A Metrics Provider that bridges Moquette metrics to IoTDB's MetricService. This implementation
 * mirrors MetricsProviderPrometheus.java to provide identical metric behavior, but forwards all
 * metrics to IoTDB's unified metric framework instead of starting a separate Prometheus HTTP
 * server.
 */
public class IoTDBMoquetteMetricsProvider implements MetricsProvider {

  public static final String TAG_ENDPOINT_PORT = "metrics_endpoint_port";
  public static final String METRIC_MOQUETTE_PUBLISHES_TOTAL = "moquette_publishes";
  public static final String METRIC_MOQUETTE_OPEN_SESSIONS = "moquette_open_sessions";
  public static final String METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX =
      "moquette_session_queue_fill_max";
  public static final String METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL = "moquette_session_messages";
  public static final String METRIC_MOQUETTE_SESSION_QUEUE_OVERRUNS_TOTAL =
      "moquette_session_queue_overruns";
  public static final String METRIC_MOQUETTE_SESSION_QUEUE_FILL = "moquette_session_queue_fill";
  private static final String METRIC_MOQUETTE_INTERCEPTOR_PUBLISH_TASKS_SUBMITTED_TOTAL =
      "moquette_interceptor_publish_tasks_submitted";
  private static final String METRIC_MOQUETTE_INTERCEPTOR_PUBLISH_TASKS_COMPLETED_TOTAL =
      "moquette_interceptor_publish_tasks_completed";
  private static final String METRIC_MOQUETTE_INTERCEPTOR_MESSAGE_SIZE_BYTES =
      "moquette_interceptor_message_size_bytes";
  private static final String METRIC_MOQUETTE_INTERCEPTOR_BACKLOG_MEMORY_BYTES =
      "moquette_interceptor_backlog_memory_bytes";

  private static final Logger LOG = LoggerFactory.getLogger(IoTDBMoquetteMetricsProvider.class);

  private int sessionQueueSize;
  private Gauge openSessionsGauge;
  private AtomicInteger[] sessionQueueFill;
  private int[] sessionQueueFillMax;
  private Counter[] sessionQueueOverrunCounters;
  private Counter[][] messageCounters;
  private Counter publishCounter;
  private Counter interceptorPublishTaskSubmittedCounter;
  private Counter interceptorPublishTaskCompletedCounter;
  private Histogram interceptorMessageSizeHistogram;
  private final AtomicLong interceptorBacklogMemoryBytes = new AtomicLong(0);

  // Wrapper class to implement reset-on-read behavior for sessionQueueFillMax
  private static class ResetOnReadInt {
    private volatile int value = 0;

    public int getAndReset() {
      int current = value;
      value = 0;
      return current;
    }

    public void updateMax(int newValue) {
      if (newValue > value) {
        value = newValue;
      }
    }
  }

  private ResetOnReadInt[] sessionQueueFillMaxWrappers;

  @Override
  public void init(IConfig config) {
    LOG.info("Initialising IoTDB Moquette metrics provider.");
    // Note: metrics_endpoint_port is ignored as metrics are exposed through IoTDB's endpoint
    int metricsPort = config.intProp(TAG_ENDPOINT_PORT, 9400);
    if (metricsPort > 0) {
      LOG.debug(
          "Metrics endpoint port {} configured but ignored - using IoTDB's Prometheus endpoint",
          metricsPort);
    }

    openSessionsGauge =
        MetricService.getInstance()
            .getOrCreateGauge(METRIC_MOQUETTE_OPEN_SESSIONS, MetricLevel.IMPORTANT);

    publishCounter =
        MetricService.getInstance()
            .getOrCreateCounter(METRIC_MOQUETTE_PUBLISHES_TOTAL, MetricLevel.IMPORTANT);

    interceptorPublishTaskSubmittedCounter =
        MetricService.getInstance()
            .getOrCreateCounter(
                METRIC_MOQUETTE_INTERCEPTOR_PUBLISH_TASKS_SUBMITTED_TOTAL, MetricLevel.IMPORTANT);

    interceptorPublishTaskCompletedCounter =
        MetricService.getInstance()
            .getOrCreateCounter(
                METRIC_MOQUETTE_INTERCEPTOR_PUBLISH_TASKS_COMPLETED_TOTAL, MetricLevel.IMPORTANT);

    // Note: IoTDB's histogram may not support custom buckets, but we'll use the default
    interceptorMessageSizeHistogram =
        MetricService.getInstance()
            .getOrCreateHistogram(
                METRIC_MOQUETTE_INTERCEPTOR_MESSAGE_SIZE_BYTES, MetricLevel.IMPORTANT);

    // This creates a callback-based gauge that reads from interceptorBacklogMemoryBytes
    MetricService.getInstance()
        .createAutoGauge(
            METRIC_MOQUETTE_INTERCEPTOR_BACKLOG_MEMORY_BYTES,
            MetricLevel.IMPORTANT,
            interceptorBacklogMemoryBytes,
            AtomicLong::get);
  }

  @Override
  public void stop() {
    LOG.info("Stopping IoTDB Moquette Metrics Provider.");
    // No cleanup needed as IoTDB MetricService manages its metric server's lifecycle.
  }

  private String labelForQueue(int id) {
    return "queue-" + id;
  }

  @Override
  public void initSessionQueues(int queueCount, int queueSize) {
    this.sessionQueueSize = queueSize;

    sessionQueueFill = new AtomicInteger[queueCount];
    sessionQueueFillMax = new int[queueCount];
    sessionQueueFillMaxWrappers = new ResetOnReadInt[queueCount];

    // Create AutoGauge for sessionQueueFill using callback-like behavior
    for (int id = 0; id < queueCount; id++) {
      sessionQueueFill[id] = new AtomicInteger();
      sessionQueueFillMax[id] = 0;
      sessionQueueFillMaxWrappers[id] = new ResetOnReadInt();

      // Create AutoGauge for each queue's fill level
      final int queueId = id;
      final AtomicInteger fill = sessionQueueFill[id];
      MetricService.getInstance()
          .createAutoGauge(
              METRIC_MOQUETTE_SESSION_QUEUE_FILL,
              MetricLevel.IMPORTANT,
              fill,
              f -> 1.0 * f.get() / sessionQueueSize,
              "queue_id",
              labelForQueue(queueId));
    }

    sessionQueueOverrunCounters = new Counter[queueCount];
    for (int id = 0; id < queueCount; id++) {
      final String label = labelForQueue(id);
      sessionQueueOverrunCounters[id] =
          MetricService.getInstance()
              .getOrCreateCounter(
                  METRIC_MOQUETTE_SESSION_QUEUE_OVERRUNS_TOTAL,
                  MetricLevel.IMPORTANT,
                  "queue_name",
                  label);
    }

    messageCounters = new Counter[queueCount][3];
    for (int id = 0; id < queueCount; id++) {
      final String label = labelForQueue(id);
      for (int qos = 0; qos <= 2; qos++) {
        messageCounters[id][qos] =
            MetricService.getInstance()
                .getOrCreateCounter(
                    METRIC_MOQUETTE_SESSION_MESSAGES_TOTAL,
                    MetricLevel.IMPORTANT,
                    "queue_name",
                    label,
                    "QoS",
                    Integer.toString(qos));
      }
    }

    // Initialize sessionQueueFillMax using AutoGauge with reset-on-read behavior
    for (int id = 0; id < queueCount; id++) {
      final int queueId = id;
      final ResetOnReadInt maxWrapper = sessionQueueFillMaxWrappers[id];
      MetricService.getInstance()
          .createAutoGauge(
              METRIC_MOQUETTE_SESSION_QUEUE_FILL_MAX,
              MetricLevel.IMPORTANT,
              maxWrapper,
              wrapper -> {
                int maxValue = wrapper.getAndReset();
                return 1.0 * maxValue / sessionQueueSize;
              },
              "queue_id",
              labelForQueue(queueId));
    }

    LOG.debug("Initialized {} session queues with size {}", queueCount, queueSize);
  }

  @Override
  public void sessionQueueInc(int queue) {
    if (queue >= sessionQueueFill.length) {
      return;
    }
    int value = sessionQueueFill[queue].incrementAndGet();
    if (value > sessionQueueFillMax[queue]) {
      sessionQueueFillMax[queue] = value;
      // Update the wrapper for reset-on-read behavior
      sessionQueueFillMaxWrappers[queue].updateMax(value);
    }
  }

  @Override
  public void sessionQueueDec(int queue) {
    if (queue >= sessionQueueFill.length) {
      return;
    }
    sessionQueueFill[queue].decrementAndGet();
  }

  @Override
  public void addSessionQueueOverrun(int queue) {
    if (queue >= sessionQueueOverrunCounters.length) {
      return;
    }
    sessionQueueOverrunCounters[queue].inc();
  }

  @Override
  public void addOpenSession() {
    openSessionsGauge.incr(1);
  }

  @Override
  public void removeOpenSession() {
    openSessionsGauge.decr(1);
  }

  @Override
  public void addPublish() {
    publishCounter.inc();
  }

  @Override
  public void addMessage(int queue, int qos) {
    if (queue < 0 || queue >= messageCounters.length) {
      return;
    }
    messageCounters[queue][qos].inc();
  }

  @Override
  public void interceptorPublishTaskSubmitted() {
    interceptorPublishTaskSubmittedCounter.inc();
  }

  @Override
  public void interceptorPublishTaskCompleted() {
    interceptorPublishTaskCompletedCounter.inc();
  }

  @Override
  public void recordInterceptorMessageSize(long sizeBytes) {
    interceptorMessageSizeHistogram.update(sizeBytes);
  }

  @Override
  public void interceptorTaskSubmittedWithSize(long payloadSizeBytes) {
    interceptorBacklogMemoryBytes.addAndGet(payloadSizeBytes);
  }

  @Override
  public void interceptorTaskCompletedWithSize(long payloadSizeBytes) {
    interceptorBacklogMemoryBytes.addAndGet(-payloadSizeBytes);
  }
}
