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

package org.apache.iotdb.commons.pipe.task.subtask;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeConnectorCriticalException;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.execution.scheduler.PipeSubtaskScheduler;
import org.apache.iotdb.commons.pipe.task.DecoratingLock;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.exception.PipeConnectionException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

public abstract class PipeTransferSubtask extends PipeReportableSubtask {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTransferSubtask.class);

  protected PipeConnector outputPipeConnector;

  protected ExecutorService subtaskCallbackListeningExecutor;

  // For controlling subtask submitting, making sure that a subtask is submitted to only one thread
  // at a time
  protected volatile boolean isSubmitted = false;
  protected final DecoratingLock callbackDecoratingLock = new DecoratingLock();

  protected PipeTransferSubtask(
      String taskID, long creationTime, PipeConnector outputPipeConnector) {
    super(taskID, creationTime);
    this.outputPipeConnector = outputPipeConnector;
  }

  @Override
  public Boolean call() throws Exception {
    final boolean hasAtLeastOneEventProcessed = super.call();

    // Wait for the callable to be decorated by Futures.addCallback in the executorService
    // to make sure that the callback can be submitted again on success or failure.
    callbackDecoratingLock.waitForDecorated();

    return hasAtLeastOneEventProcessed;
  }

  @Override
  public void bindExecutors(
      ListeningExecutorService subtaskWorkerThreadPoolExecutor,
      ExecutorService subtaskCallbackListeningExecutor,
      PipeSubtaskScheduler subtaskScheduler) {
    this.subtaskWorkerThreadPoolExecutor = subtaskWorkerThreadPoolExecutor;
    this.subtaskCallbackListeningExecutor = subtaskCallbackListeningExecutor;
    this.subtaskScheduler = subtaskScheduler;
  }

  /**
   * Submit a {@link PipeSubtask} to the executor to keep it running. Note that the function will be
   * called when connector starts or the subTask finishes the last round, Thus the {@link
   * PipeTransferSubtask#isSubmitted} sign is added to avoid concurrent problem of the two, ensuring
   * two or more submitting threads generates only one winner.
   */
  @Override
  public synchronized void submitSelf() {
    if (shouldStopSubmittingSelf.get() || isSubmitted) {
      return;
    }

    callbackDecoratingLock.markAsDecorating();
    try {
      final ListenableFuture<Boolean> nextFuture = subtaskWorkerThreadPoolExecutor.submit(this);
      Futures.addCallback(nextFuture, this, subtaskCallbackListeningExecutor);
      isSubmitted = true;
    } finally {
      callbackDecoratingLock.markAsDecorated();
    }
  }

  @Override
  public synchronized void onSuccess(Boolean hasAtLeastOneEventProcessed) {
    isSubmitted = false;

    super.onSuccess(hasAtLeastOneEventProcessed);
  }

  @Override
  public synchronized void onFailure(Throwable throwable) {
    isSubmitted = false;

    if (isClosed.get()) {
      LOGGER.info("onFailure in pipe transfer, ignored because pipe is dropped.");
      releaseLastEvent(false);
      return;
    }

    // Retry to connect to the target system if the connection is broken
    if (throwable instanceof PipeConnectionException) {
      LOGGER.warn(
          "PipeConnectionException occurred, retrying to connect to the target system...",
          throwable);

      int retry = 0;
      while (retry < MAX_RETRY_TIMES) {
        try {
          outputPipeConnector.handshake();
          LOGGER.info("Successfully reconnected to the target system.");
          break;
        } catch (Exception e) {
          retry++;
          LOGGER.warn(
              "Failed to reconnect to the target system, retrying ... "
                  + "after [{}/{}] time(s) retries.",
              retry,
              MAX_RETRY_TIMES,
              e);
          try {
            Thread.sleep(retry * PipeConfig.getInstance().getPipeConnectorRetryIntervalMs());
          } catch (InterruptedException interruptedException) {
            LOGGER.info(
                "Interrupted while sleeping, perhaps need to check "
                    + "whether the thread is interrupted.",
                interruptedException);
            Thread.currentThread().interrupt();
          }
        }
      }

      // Stop current pipe task if failed to reconnect to the target system after MAX_RETRY_TIMES
      // times
      if (retry == MAX_RETRY_TIMES) {
        if (lastEvent instanceof EnrichedEvent) {
          LOGGER.warn(
              "Failed to reconnect to the target system after {} times, "
                  + "stopping current pipe task {}... "
                  + "Status shown when query the pipe will be 'STOPPED'. "
                  + "Please restart the task by executing 'START PIPE' manually if needed.",
              MAX_RETRY_TIMES,
              taskID,
              throwable);

          String rootCause = getRootCause(throwable);
          report(
              ((EnrichedEvent) lastEvent),
              new PipeRuntimeConnectorCriticalException(
                  throwable.getMessage()
                      + (Objects.nonNull(rootCause) ? ", root cause: " + rootCause : "")));
        } else {
          LOGGER.error(
              "Failed to reconnect to the target system after {} times, "
                  + "stopping current pipe task {} locally... "
                  + "Status shown when query the pipe will be 'RUNNING' instead of 'STOPPED', "
                  + "but the task is actually stopped. "
                  + "Please restart the task by executing 'START PIPE' manually if needed.",
              MAX_RETRY_TIMES,
              taskID,
              throwable);
        }

        // Although the pipe task will be stopped, we still don't release the last event here
        // Because we need to keep it for the next retry. If user wants to restart the task,
        // the last event will be processed again. The last event will be released when the task
        // is dropped or the process is running normally.

        // Stop current pipe task if failed to reconnect to the target system after MAX_RETRY_TIMES
        return;
      }
    } else {
      LOGGER.warn(
          "A non-PipeConnectionException occurred, exception message: {}",
          throwable.getMessage(),
          throwable);
    }

    // Handle other exceptions as usual
    super.onFailure(new PipeRuntimeConnectorCriticalException(throwable.getMessage()));
  }
}
