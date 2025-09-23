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

package org.apache.iotdb.commons.systeminfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class LinuxSystemInfoGetter extends SystemInfoGetter {

  private static final Logger logger = LoggerFactory.getLogger(LinuxSystemInfoGetter.class);

  private static final String BASH = "/bin/bash";

  @Override
  Logger getLogger() {
    return logger;
  }

  @Override
  String getCPUIdImpl() throws IOException {
    return executeShell(
        new String[] {
          BASH, "-c", "dmidecode -t processor | grep 'ID' | awk -F ':' '{print $2}' | head -n 1"
        });
  }

  @Override
  String getMainBoardIdImpl() throws IOException {
    return executeShell(
        new String[] {
          BASH, "-c", "dmidecode | grep 'Serial Number' | awk -F ':' '{print $2}' | head -n 1"
        });
  }

  @Override
  String getSystemUUIDImpl() throws IOException {
    return executeShell(new String[] {BASH, "-c", "cat /sys/class/dmi/id/product_uuid"});
  }

  protected String executeShell(String[] shell) throws IOException {
    Process process = Runtime.getRuntime().exec(shell);
    process.getOutputStream().close();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      return reader.readLine().trim();
    }
  }
}
