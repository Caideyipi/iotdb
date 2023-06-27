package org.apache.iotdb.db.protocol.metrics;

import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.storageengine.dataregion.migration.MigrationCause;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.impl.DoNothingMetricManager;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MigrationMetrics implements IMetricSet {
  private static final MigrationMetrics INSTANCE = new MigrationMetrics();

  public static final String MIGRATION_CAUSE = "migration_cause";
  public static final String MIGRATION_FILE_SIZE = "migration_file_size";
  public static final String MIGRATION_TOTAL_TIME = "migration_total_time";
  public static final String MIGRATION_FILE_COPY_TIME = "migration_file_copy_time";
  public static final String MIGRATION_WAIT_LOCK_TIME = "migration_wait_lock_time";

  private final Map<MigrationCause, Histogram> migrationCause = new HashMap<>();
  private final Map<String, Histogram> destTierType2MigrationFileSize = new ConcurrentHashMap<>();
  private final Map<String, Timer> destTierType2MigrationTimer = new ConcurrentHashMap<>();
  private final Map<String, Timer> destTierType2MigrationFileCopyTimer = new ConcurrentHashMap<>();
  private final Map<String, Timer> destTierType2MigrationWaitLockTimer = new ConcurrentHashMap<>();

  private MigrationMetrics() {}

  private String getDestTierType(int tierLevel, boolean toLocal) {
    return String.format("to_tier%d(%s)", tierLevel, toLocal ? "local" : "remote");
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    for (MigrationCause cause : MigrationCause.values()) {
      migrationCause.put(
          cause,
          metricService.getOrCreateHistogram(
              Metric.MIGRATION_TASK_CAUSE.toString(),
              MetricLevel.IMPORTANT,
              Tag.NAME.toString(),
              MIGRATION_CAUSE,
              Tag.TYPE.toString(),
              cause.toString()));
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    for (MigrationCause cause : migrationCause.keySet()) {
      metricService.remove(
          MetricType.HISTOGRAM,
          Metric.MIGRATION_TASK_CAUSE.toString(),
          Tag.NAME.toString(),
          MIGRATION_CAUSE,
          Tag.TYPE.toString(),
          cause.toString());
    }
    migrationCause.clear();

    for (String destTierType : destTierType2MigrationFileSize.keySet()) {
      metricService.remove(
          MetricType.HISTOGRAM,
          Metric.MIGRATION_TASK_SIZE.toString(),
          Tag.NAME.toString(),
          MIGRATION_FILE_SIZE,
          Tag.TYPE.toString(),
          destTierType);
    }
    destTierType2MigrationFileSize.clear();

    for (String destTierType : destTierType2MigrationTimer.keySet()) {
      metricService.remove(
          MetricType.TIMER,
          Metric.MIGRATION_TASK_COST.toString(),
          Tag.NAME.toString(),
          MIGRATION_TOTAL_TIME,
          Tag.TYPE.toString(),
          destTierType);
    }
    destTierType2MigrationTimer.clear();

    for (String destTierType : destTierType2MigrationFileCopyTimer.keySet()) {
      metricService.remove(
          MetricType.TIMER,
          Metric.MIGRATION_TASK_COST.toString(),
          Tag.NAME.toString(),
          MIGRATION_FILE_COPY_TIME,
          Tag.TYPE.toString(),
          destTierType);
    }
    destTierType2MigrationFileCopyTimer.clear();

    for (String destTierType : destTierType2MigrationWaitLockTimer.keySet()) {
      metricService.remove(
          MetricType.TIMER,
          Metric.MIGRATION_TASK_COST.toString(),
          Tag.NAME.toString(),
          MIGRATION_WAIT_LOCK_TIME,
          Tag.TYPE.toString(),
          destTierType);
    }
    destTierType2MigrationWaitLockTimer.clear();
  }

  public void recordMigrationCause(MigrationCause cause) {
    migrationCause.getOrDefault(cause, DoNothingMetricManager.DO_NOTHING_HISTOGRAM).update(1);
  }

  public void recordMigrationFileSize(int destTierLevel, boolean toLocal, long fileSize) {
    String destTierType = getDestTierType(destTierLevel, toLocal);
    destTierType2MigrationFileSize
        .computeIfAbsent(
            destTierType,
            k ->
                MetricService.getInstance()
                    .getOrCreateHistogram(
                        Metric.MIGRATION_TASK_SIZE.toString(),
                        MetricLevel.IMPORTANT,
                        Tag.NAME.toString(),
                        MIGRATION_FILE_SIZE,
                        Tag.TYPE.toString(),
                        destTierType))
        .update(fileSize);
  }

  public void recordMigrationTotalTime(int destTierLevel, boolean toLocal, long costTimeInNanos) {
    String destTierType = getDestTierType(destTierLevel, toLocal);
    destTierType2MigrationTimer
        .computeIfAbsent(
            destTierType,
            k ->
                MetricService.getInstance()
                    .getOrCreateTimer(
                        Metric.MIGRATION_TASK_COST.toString(),
                        MetricLevel.IMPORTANT,
                        Tag.NAME.toString(),
                        MIGRATION_TOTAL_TIME,
                        Tag.TYPE.toString(),
                        destTierType))
        .updateNanos(costTimeInNanos);
  }

  public void recordMigrationFileCopyTime(
      int destTierLevel, boolean toLocal, long costTimeInNanos) {
    String destTierType = getDestTierType(destTierLevel, toLocal);
    destTierType2MigrationFileCopyTimer
        .computeIfAbsent(
            destTierType,
            k ->
                MetricService.getInstance()
                    .getOrCreateTimer(
                        Metric.MIGRATION_TASK_COST.toString(),
                        MetricLevel.IMPORTANT,
                        Tag.NAME.toString(),
                        MIGRATION_FILE_COPY_TIME,
                        Tag.TYPE.toString(),
                        destTierType))
        .updateNanos(costTimeInNanos);
  }

  public void recordMigrationWaitLockTime(
      int destTierLevel, boolean toLocal, long costTimeInNanos) {
    String destTierType = getDestTierType(destTierLevel, toLocal);
    destTierType2MigrationWaitLockTimer
        .computeIfAbsent(
            destTierType,
            k ->
                MetricService.getInstance()
                    .getOrCreateTimer(
                        Metric.MIGRATION_TASK_COST.toString(),
                        MetricLevel.IMPORTANT,
                        Tag.NAME.toString(),
                        MIGRATION_WAIT_LOCK_TIME,
                        Tag.TYPE.toString(),
                        destTierType))
        .updateNanos(costTimeInNanos);
  }

  public static MigrationMetrics getInstance() {
    return INSTANCE;
  }
}
