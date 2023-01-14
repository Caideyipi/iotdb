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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.timecho.timechodb.rpc;

import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;

import com.giladam.listmatch.ListMatcher;
import com.giladam.listmatch.PatternList;
import com.timecho.timechodb.conf.ConfigFileLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class IPFilter {
  private static Logger logger = LoggerFactory.getLogger(IPFilter.class);
  static IPFilter INSTANCE = new IPFilter();

  public static IPFilter getInstance() {
    return INSTANCE;
  }

  ScheduledExecutorService service;
  Path whiteListFile;

  Set<String> allowListPatterns;

  private IPFilter() {
    whiteListFile = ConfigFileLoader.getPropsUrl("white.list");
    init();
    // 每1分钟，同步一次白名单。
    service = Executors.newSingleThreadScheduledExecutor();
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        service, this::checkAndUpdateWhiteList, 1, 60, TimeUnit.SECONDS);
  }

  PatternList pattern;
  long lastModification = 0;

  ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  void init() {
    if (whiteListFile != null) {
      try {
        File file = whiteListFile.toFile();
        if (file.exists()) {
          lastModification = Math.max(lastModification, file.lastModified());
          allowListPatterns = ListMatcher.readPatternsFromFile(file);
          pattern = new PatternList(allowListPatterns, ".", false);
        } else {
          logger.info("Whitelist not found.");
          pattern = null;
        }
      } catch (IOException e) {
        logger.error("load whitelist failed.", e);
      }
    } else {
      logger.info("Whitelist not found.");
    }
  }

  public static boolean isInWhiteList(String ip) {
    if (INSTANCE.pattern == null) {
      return false;
    }
    INSTANCE.lock.readLock().lock();
    try {
      return INSTANCE.pattern.matches(ip);
    } finally {
      INSTANCE.lock.readLock().unlock();
    }
  }

  public void checkAndUpdateWhiteList() {
    if (whiteListFile == null) {
      whiteListFile = ConfigFileLoader.getPropsUrl("white.list");
    }
    if (whiteListFile != null) {
      // 检查文件是否被修改。如果存在被修改的，就同步。否则不同步。
      try {
        File file = whiteListFile.toFile();
        if (file.exists()) {
          if (file.lastModified() > lastModification) {
            updateWhiteList();
          }
        } else {
          // 文件不存在，要更新内存中的值
          updateWhiteList();
        }
      } catch (Exception e) {
        logger.error("error when check whether whitelist is update: ", e);
      }
    }
  }

  public void updateWhiteList() {
    logger.info("whitelist has changed, will reload...");
    lock.writeLock().lock();
    try {
      init();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Set<String> getAllowListPatterns() {
    return allowListPatterns;
  }

  public void setAllowListPatterns(Set<String> allowListPatterns) {
    try (FileWriter fileWriter = new FileWriter(whiteListFile.toFile())) {
      for (String allowListPattern : allowListPatterns) {
        fileWriter.write(allowListPattern);
        fileWriter.write(System.getProperty("line.separator"));
      }
    } catch (IOException e) {
      logger.error("upadte white.list error,", e);
    }
    checkAndUpdateWhiteList();
  }
}
