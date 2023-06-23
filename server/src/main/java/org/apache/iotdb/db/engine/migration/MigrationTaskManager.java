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
package org.apache.iotdb.db.engine.migration;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.directories.TierManager;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceStatus;
import org.apache.iotdb.db.service.metrics.MigrationMetrics;
import org.apache.iotdb.db.utils.DateTimeUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MigrationTaskManager implements IService {
  private static final Logger logger = LoggerFactory.getLogger(MigrationTaskManager.class);
  private static final IoTDBConfig iotdbConfig = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private static final TierManager tierManager = TierManager.getInstance();
  private static final long CHECK_INTERVAL_IN_SECONDS = 10;
  private static final double MOVE_THRESHOLD_SAFE_LIMIT = 0.1;
  private static final int MIGRATION_TASK_LIMIT = 20;
  /** max concurrent migration tasks */
  private final AtomicInteger migrationTasksNum = new AtomicInteger(0);
  /** single thread to schedule */
  private ScheduledExecutorService scheduler;
  /** single thread to sync syncingBuffer to disk */
  private ExecutorService workers;

  @Override
  public void start() throws StartupException {
    if (iotdbConfig.getTierDataDirs().length == 1) {
      logger.info("tiered storage status: disable");
      return;
    }
    // metrics
    MetricService.getInstance().addMetricSet(MigrationMetrics.getInstance());
    // threads and tasks
    scheduler =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.MIGRATION_SCHEDULER.getName());
    workers =
        IoTDBThreadPoolFactory.newFixedThreadPool(
            iotdbConfig.getMigrateThreadCount(), ThreadName.MIGRATION.getName());
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        scheduler,
        () -> new MigrationScheduleTask().run(),
        CHECK_INTERVAL_IN_SECONDS,
        CHECK_INTERVAL_IN_SECONDS,
        TimeUnit.SECONDS);
  }

  private class MigrationScheduleTask implements Runnable {
    private final long[] tierDiskTotalSpace = tierManager.getTierDiskTotalSpace();
    private final long[] tierDiskUsableSpace = tierManager.getTierDiskUsableSpace();
    /** Tiers need migrating data to the next tier */
    private final Set<Integer> needMigrationTiers = new HashSet<>();
    /** Tiers are disk-full, cannot migrate data to these tiers */
    private final Set<Integer> spaceWarningTiers = new HashSet<>();

    public MigrationScheduleTask() {
      for (int i = 0; i < tierManager.getTiersNum(); i++) {
        double usable = tierDiskUsableSpace[i] * 1.0 / tierDiskTotalSpace[i];
        if (usable <= iotdbConfig.getSpaceMoveThresholds()[i]) {
          needMigrationTiers.add(i);
        }
        if (usable <= commonConfig.getDiskSpaceWarningThreshold()) {
          spaceWarningTiers.add(i);
        }
      }
    }

    @Override
    public void run() {
      schedule();
    }

    private void schedule() {
      if (migrationTasksNum.get() >= MIGRATION_TASK_LIMIT) {
        return;
      }
      // sort all tsfiles by priority
      List<TsFileResource> tsfiles = new ArrayList<>();
      for (DataRegion dataRegion : StorageEngine.getInstance().getAllDataRegions()) {
        tsfiles.addAll(dataRegion.getSequenceFileList());
        tsfiles.addAll(dataRegion.getUnSequenceFileList());
      }
      // only migrate closed TsFiles not in the last tier
      tsfiles =
          tsfiles.stream()
              .filter(
                  f ->
                      f.getStatus() == TsFileResourceStatus.NORMAL
                          && f.getTierLevel() + 1 < iotdbConfig.getTierDataDirs().length)
              .sorted(this::compareMigrationPriority)
              .collect(Collectors.toList());
      // submit migration tasks
      for (TsFileResource tsfile : tsfiles) {
        if (migrationTasksNum.get() >= MIGRATION_TASK_LIMIT) {
          break;
        }
        try {
          int currentTier = tsfile.getTierLevel();
          int nextTier = currentTier + 1;
          // skip migration when next tier is full
          if (spaceWarningTiers.contains(nextTier)) {
            continue;
          }
          // check tier ttl and disk space
          long tierTTL =
              DateTimeUtils.convertMilliTimeWithPrecision(
                  System.currentTimeMillis() - commonConfig.getTierTTLInMs()[currentTier],
                  commonConfig.getTimestampPrecision());
          if (!tsfile.stillLives(tierTTL)) {
            trySubmitMigrationTask(
                currentTier,
                MigrationCause.TTL,
                tsfile,
                tierManager.getNextFolderForTsFile(nextTier, tsfile.isSeq()));
          } else if (needMigrationTiers.contains(currentTier)) {
            trySubmitMigrationTask(
                currentTier,
                MigrationCause.DISK_SPACE,
                tsfile,
                tierManager.getNextFolderForTsFile(nextTier, tsfile.isSeq()));
          }
        } catch (Exception e) {
          logger.error(
              "An error occurred when check and try to migrate TsFileResource {}", tsfile, e);
        }
      }
    }

    private void trySubmitMigrationTask(
        int tierLevel, MigrationCause cause, TsFileResource sourceTsFile, String targetDir)
        throws IOException {
      if (!sourceTsFile.setStatus(TsFileResourceStatus.MIGRATING)) {
        return;
      }
      migrationTasksNum.incrementAndGet();
      MigrationTask task = MigrationTask.newTask(cause, sourceTsFile, targetDir);
      workers.submit(task);
      tierDiskUsableSpace[tierLevel] -= sourceTsFile.getTsFileSize();
      if (needMigrationTiers.contains(tierLevel)) {
        double usable = tierDiskUsableSpace[tierLevel] * 1.0 / tierDiskTotalSpace[tierLevel];
        if (usable > iotdbConfig.getSpaceMoveThresholds()[tierLevel] + MOVE_THRESHOLD_SAFE_LIMIT) {
          needMigrationTiers.remove(tierLevel);
        }
      }
    }

    private int compareMigrationPriority(TsFileResource f1, TsFileResource f2) {
      // lower tier first
      int res = Integer.compare(f1.getTierLevel(), f2.getTierLevel());
      // old time partitions first
      if (res == 0) {
        res = Long.compare(f1.getTimePartition(), f2.getTimePartition());
      }
      // sequence files in one partition
      if (res == 0) {
        if (f1.isSeq() && !f2.isSeq()) {
          res = -1;
        } else if (!f1.isSeq() && f2.isSeq()) {
          res = 1;
        }
      }
      // old version files in one partition
      if (res == 0) {
        res = Long.compare(f1.getVersion(), f2.getVersion());
      }
      return res;
    }
  }

  void decreaseMigrationTasksNum() {
    migrationTasksNum.decrementAndGet();
  }

  @Override
  public void stop() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    if (workers != null) {
      workers.shutdownNow();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MIGRATION_SERVICE;
  }

  public static MigrationTaskManager getInstance() {
    return InstanceHolder.INSTANCE;
  }

  private static class InstanceHolder {
    private InstanceHolder() {}

    private static final MigrationTaskManager INSTANCE = new MigrationTaskManager();
  }
}
