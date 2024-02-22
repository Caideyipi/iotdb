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

package org.apache.iotdb.db.pipe.event.common.tsfile;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;
import org.apache.iotdb.db.pipe.resource.PipeResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class PipeTsFileInsertionEvent extends EnrichedEvent implements TsFileInsertionEvent {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTsFileInsertionEvent.class);

  private boolean isTsFileFormatValid = true;

  private final TsFileResource resource;
  private File tsFile;

  boolean withMod;
  private File modFile;

  private final boolean isLoaded;
  private final boolean isGeneratedByPipe;

  private final AtomicBoolean isClosed;
  private TsFileInsertionDataContainer dataContainer;

  public PipeTsFileInsertionEvent(
      TsFileResource resource, boolean isLoaded, boolean isGeneratedByPipe) {
    this(
        resource,
        isLoaded,
        isGeneratedByPipe,
        false,
        null,
        null,
        null,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  public PipeTsFileInsertionEvent(
      TsFileResource resource,
      boolean isLoaded,
      boolean isGeneratedByPipe,
      boolean withMod,
      String pipeName,
      PipeTaskMeta pipeTaskMeta,
      String pattern,
      long startTime,
      long endTime) {
    super(pipeName, pipeTaskMeta, pattern, startTime, endTime);

    this.resource = resource;
    tsFile = resource.getTsFile();

    this.isLoaded = isLoaded;
    this.isGeneratedByPipe = isGeneratedByPipe;
    this.withMod = withMod;

    isClosed = new AtomicBoolean(resource.isClosed());
    // register close listener if TsFile is not closed
    if (!isClosed.get()) {
      final TsFileProcessor processor = resource.getProcessor();
      if (processor != null) {
        processor.addCloseFileListener(
            o -> {
              synchronized (isClosed) {
                isTsFileFormatValid = o.isTsFileFormatValidForPipe();
                isClosed.set(true);
                isClosed.notifyAll();
              }
            });
      }
    }
    // check again after register close listener in case TsFile is closed during the process
    isClosed.set(resource.isClosed());
  }

  /**
   * @return {@code false} if this file can't be sent by pipe due to format violations. {@code true}
   *     otherwise.
   */
  public boolean waitForTsFileClose() throws InterruptedException {
    if (!isClosed.get()) {
      synchronized (isClosed) {
        while (!isClosed.get()) {
          isClosed.wait();
        }
      }
    }
    return isTsFileFormatValid;
  }

  public File getTsFile() {
    return tsFile;
  }

  public File getModFile() {
    return modFile;
  }

  public boolean getIsLoaded() {
    return isLoaded;
  }

  public long getFileStartTime() {
    return resource.getFileStartTime();
  }

  /////////////////////////// EnrichedEvent ///////////////////////////

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    try {
      tsFile = PipeResourceManager.tsfile().increaseFileReference(tsFile, true);
      if (withMod && resource.getModFile().exists()) {
        modFile =
            PipeResourceManager.tsfile()
                .increaseFileReference(new File(resource.getModFile().getFilePath()), false);
      }
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Increase reference count for TsFile %s error. Holder Message: %s",
              tsFile.getPath(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    try {
      PipeResourceManager.tsfile().decreaseFileReference(tsFile);
      if (Objects.nonNull(modFile)) {
        PipeResourceManager.tsfile().decreaseFileReference(modFile);
      }
      return true;
    } catch (Exception e) {
      LOGGER.warn(
          String.format(
              "Decrease reference count for TsFile %s error. Holder Message: %s",
              tsFile.getPath(), holderMessage),
          e);
      return false;
    }
  }

  @Override
  public ProgressIndex getProgressIndex() {
    try {
      waitForTsFileClose();
      return resource.getMaxProgressIndexAfterClose();
    } catch (InterruptedException e) {
      LOGGER.warn(
          String.format(
              "Interrupted when waiting for closing TsFile %s.", resource.getTsFilePath()));
      Thread.currentThread().interrupt();
      return MinimumProgressIndex.INSTANCE;
    }
  }

  @Override
  public PipeTsFileInsertionEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName, PipeTaskMeta pipeTaskMeta, String pattern, long startTime, long endTime) {
    return new PipeTsFileInsertionEvent(
        resource,
        isLoaded,
        isGeneratedByPipe,
        withMod,
        pipeName,
        pipeTaskMeta,
        pattern,
        startTime,
        endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  @Override
  public boolean isEventTimeOverlappedWithTimeRange() {
    return startTime <= resource.getFileEndTime() && resource.getFileStartTime() <= endTime;
  }

  /////////////////////////// TsFileInsertionEvent ///////////////////////////

  @Override
  public boolean shouldParseTimeOrPattern() {
    boolean shouldParseTimeOrPattern = false;
    try {
      shouldParseTimeOrPattern = super.shouldParseTimeOrPattern();
      return shouldParseTimeOrPattern;
    } finally {
      // Super method will call shouldParsePattern() and then init dataContainer at
      // shouldParsePattern(). If shouldParsePattern() returns false, dataContainer will
      // not be used, so we need to close the resource here.
      if (!shouldParseTimeOrPattern) {
        close();
      }
    }
  }

  @Override
  public boolean shouldParsePattern() {
    return super.shouldParsePattern() && initDataContainer().shouldParsePattern();
  }

  @Override
  public Iterable<TabletInsertionEvent> toTabletInsertionEvents() {
    return initDataContainer().toTabletInsertionEvents();
  }

  private TsFileInsertionDataContainer initDataContainer() {
    try {
      if (dataContainer == null) {
        waitForTsFileClose();
        dataContainer =
            new TsFileInsertionDataContainer(
                tsFile, getPattern(), startTime, endTime, pipeTaskMeta, this);
      }
      return dataContainer;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      close();

      final String errorMsg =
          String.format(
              "Interrupted when waiting for closing TsFile %s.", resource.getTsFilePath());
      LOGGER.warn(errorMsg, e);
      throw new PipeException(errorMsg);
    } catch (IOException e) {
      close();

      final String errorMsg = String.format("Read TsFile %s error.", resource.getTsFilePath());
      LOGGER.warn(errorMsg, e);
      throw new PipeException(errorMsg);
    }
  }

  /** Release the resource of data container. */
  @Override
  public void close() {
    if (dataContainer != null) {
      dataContainer.close();
      dataContainer = null;
    }
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeTsFileInsertionEvent{isTsFileFormatValid=%s, resource=%s, tsFile=%s, isLoaded=%s, isGeneratedByPipe=%s, isClosed=%s, dataContainer=%s}",
            isTsFileFormatValid,
            resource,
            tsFile,
            isLoaded,
            isGeneratedByPipe,
            isClosed.get(),
            dataContainer)
        + " - "
        + super.toString();
  }
}
