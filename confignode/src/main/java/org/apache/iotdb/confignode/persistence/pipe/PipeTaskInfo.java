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
package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.task.meta.ConfigNodePipeTaskMetaKeeper;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.sync.pipe.PipeStatus;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

public class PipeTaskInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskInfo.class);
  private static final ConfigNodeConfig CONFIG_NODE_CONF =
      ConfigNodeDescriptor.getInstance().getConf();
  private static final String SNAPSHOT_FILE_NAME = "pipe_task_info.bin";

  private final ReentrantLock pipeTaskInfoLock = new ReentrantLock();

  private final ConfigNodePipeTaskMetaKeeper pipeTaskMetaKeeper;

  public PipeTaskInfo() throws IOException {
    pipeTaskMetaKeeper = new ConfigNodePipeTaskMetaKeeper();
  }

  /////////////////////////////// Lock ///////////////////////////////

  public void acquirePipeTaskInfoLock() {
    pipeTaskInfoLock.lock();
  }

  public void releasePipeTaskInfoLock() {
    pipeTaskInfoLock.unlock();
  }

  /////////////////////////////// Validator ///////////////////////////////

  public boolean existTaskName(String pipeName) {
    return pipeTaskMetaKeeper.containsPipeTask(pipeName);
  }

  public PipeStatus getPipeStatus(String pipeName) {
    return pipeTaskMetaKeeper.getPipeTaskMeta(pipeName).getStatus();
  }

  /////////////////////////////// Pipe Task Management ///////////////////////////////

  public TSStatus createPipe(CreatePipePlanV2 plan) {
    pipeTaskMetaKeeper.addPipeTaskMeta(plan.getPipeTaskMeta());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus setPipeStatus(SetPipeStatusPlanV2 plan) {
    pipeTaskMetaKeeper.getPipeTaskMeta(plan.getPipeName()).setStatus(plan.getPipeStatus());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus dropPipe(DropPipePlanV2 plan) {
    pipeTaskMetaKeeper.removePipeTaskMeta(plan.getPipeName());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /////////////////////////////// Snapshot Processor ///////////////////////////////
  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    acquirePipeTaskInfoLock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      pipeTaskMetaKeeper.processTakeSnapshot(fileOutputStream);
    } finally {
      releasePipeTaskInfoLock();
    }
    return true;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILE_NAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    acquirePipeTaskInfoLock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
      pipeTaskMetaKeeper.processLoadSnapshot(fileInputStream);
    } finally {
      releasePipeTaskInfoLock();
    }
  }
}
