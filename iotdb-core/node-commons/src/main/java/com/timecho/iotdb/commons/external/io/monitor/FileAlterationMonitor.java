/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.timecho.iotdb.commons.external.io.monitor;

import com.timecho.iotdb.commons.external.io.ThreadUtils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;

/**
 * A runnable that spawns a monitoring thread triggering any registered {@link
 * FileAlterationObserver} at a specified interval.
 *
 * @see FileAlterationObserver
 * @since 2.0
 */
public final class FileAlterationMonitor implements Runnable {

  private final long intervalMillis;
  private final List<FileAlterationObserver> observers = new CopyOnWriteArrayList<>();
  private Thread thread;
  private ThreadFactory threadFactory;
  private volatile boolean running;

  /**
   * Constructs a monitor with the specified interval.
   *
   * @param intervalMillis The amount of time in milliseconds to wait between checks of the file
   *     system.
   */
  public FileAlterationMonitor(final long intervalMillis) {
    this.intervalMillis = intervalMillis;
  }

  /**
   * Adds a file system observer to this monitor.
   *
   * @param observer The file system observer to add
   */
  public void addObserver(final FileAlterationObserver observer) {
    if (observer != null) {
      observers.add(observer);
    }
  }

  /**
   * Returns the interval.
   *
   * @return the interval
   */
  public long getInterval() {
    return intervalMillis;
  }

  /** Runs this monitor. */
  @Override
  public void run() {
    while (running) {
      observers.forEach(FileAlterationObserver::checkAndNotify);
      if (!running) {
        break;
      }
      try {
        ThreadUtils.sleep(Duration.ofMillis(intervalMillis));
      } catch (final InterruptedException ignored) {
        // ignore
      }
    }
  }

  /**
   * Starts monitoring.
   *
   * @throws Exception if an error occurs initializing the observer
   */
  public synchronized void start() throws Exception {
    if (running) {
      throw new IllegalStateException("Monitor is already running");
    }
    for (final FileAlterationObserver observer : observers) {
      observer.initialize();
    }
    running = true;
    if (threadFactory != null) {
      thread = threadFactory.newThread(this);
    } else {
      thread = new Thread(this);
    }
    thread.start();
  }

  /**
   * Stops monitoring.
   *
   * @throws Exception if an error occurs initializing the observer
   */
  public synchronized void stop() throws Exception {
    stop(intervalMillis);
  }

  /**
   * Stops monitoring.
   *
   * @param stopInterval the amount of time in milliseconds to wait for the thread to finish. A
   *     value of zero will wait until the thread is finished (see {@link Thread#join(long)}).
   * @throws Exception if an error occurs initializing the observer
   * @since 2.1
   */
  public synchronized void stop(final long stopInterval) throws Exception {
    if (!running) {
      throw new IllegalStateException("Monitor is not running");
    }
    running = false;
    try {
      thread.interrupt();
      thread.join(stopInterval);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    for (final FileAlterationObserver observer : observers) {
      observer.destroy();
    }
  }
}
