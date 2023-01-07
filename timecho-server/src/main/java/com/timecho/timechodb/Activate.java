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
package com.timecho.timechodb;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.timecho.timechodb.conf.TimechoDBDescriptor;
import com.timecho.timechodb.license.LicenseException;
import com.timecho.timechodb.license.LicenseManager;
import com.timecho.timechodb.license.MachineCodeManager;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintStream;
import java.util.Scanner;

import static com.timecho.timechodb.license.LicenseManager.LICENSE_FILE_PATH;
import static com.timecho.timechodb.license.LicenseManager.LICENSE_PATH;

public class Activate {
  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);

  public static void main(String[] args) {
    try {
      LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
      loggerContext.getLoggerList().forEach(x -> x.setLevel(Level.ERROR));
      TimechoDBDescriptor.getInstance().loadTimechoDBProperties();
      // init licenseManager and machineCodeManager
      LicenseManager licenseManager = LicenseManager.getInstance();
      MachineCodeManager machineCodeManager = MachineCodeManager.getInstance();
      machineCodeManager.init();
      String systemInfo = machineCodeManager.getMachineCodeCipher();
      File licenseFile = new File(LICENSE_FILE_PATH);
      String licenseStr;
      if (!licenseFile.exists()) {
        SCREEN_PRINTER.println(
            "\t\n=============Copy the machine code and contact the service  provider==============\t\n"
                + systemInfo
                + "\t\n=================================================================================\t\nEnter the activation code on the CLI:");
        Scanner scanner = new Scanner(System.in);
        licenseStr = scanner.next();

        File licenseDirectoryFile = new File(LICENSE_PATH);
        // If this is the first startup, license file is not exists,need to activate.else read from
        // license file.
        if (!licenseDirectoryFile.exists() && !licenseDirectoryFile.mkdirs()) {
          SCREEN_PRINTER.println("create directory error, insufficient permissions.");
          return;
        }
        licenseManager.loadLicense(licenseStr);
        if (!licenseManager.verify(machineCodeManager.getMachineCode())) {
          SCREEN_PRINTER.println("license verify error,please check license");
          return;
        }
        licenseManager.write(licenseStr, LICENSE_FILE_PATH);
        SCREEN_PRINTER.println("TimechoDB activate successfully!");
      } else {
        SCREEN_PRINTER.println(
            "license file already exist,location of the disk is:" + licenseFile.getAbsolutePath());
      }
    } catch (LicenseException e) {
      SCREEN_PRINTER.println(e.getMessage());
    }
  }
}
