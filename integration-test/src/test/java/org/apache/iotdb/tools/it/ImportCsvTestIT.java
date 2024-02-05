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

package org.apache.iotdb.tools.it;

import org.apache.iotdb.cli.it.AbstractScript;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

<<<<<<< HEAD:integration-test/src/test/java/org/apache/iotdb/tools/it/ImportCsvTestIT.java
import org.junit.AfterClass;
import org.junit.BeforeClass;
=======
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
>>>>>>> 6943524b000217bf6d4678b51097f93cfedad8f3:iotdb-client/cli/src/test/java/org/apache/iotdb/tool/integration/ImportCsvTestIT.java
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;

<<<<<<< HEAD:integration-test/src/test/java/org/apache/iotdb/tools/it/ImportCsvTestIT.java
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
=======
/*! The only way this test can pass, is if any of the tests in the previous build have left an instance of IoTDB running */
@Ignore("This test has been moved to the Integration-Test module")
>>>>>>> 6943524b000217bf6d4678b51097f93cfedad8f3:iotdb-client/cli/src/test/java/org/apache/iotdb/tool/integration/ImportCsvTestIT.java
public class ImportCsvTestIT extends AbstractScript {

  private static String ip;

  private static String port;

  private static String toolsPath;

  private static String libPath;

  @BeforeClass
  public static void setUp() {
    EnvFactory.getEnv().initClusterEnvironment();
    ip = EnvFactory.getEnv().getIP();
    port = EnvFactory.getEnv().getPort();
    toolsPath = EnvFactory.getEnv().getToolsPath();
    libPath = EnvFactory.getEnv().getLibPath();
  }

  @AfterClass
  public static void tearDown() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // TODO: add real tests
  @Test
  public void test() throws IOException {
    String os = System.getProperty("os.name").toLowerCase();
    if (os.startsWith("windows")) {
      testOnWindows();
    } else {
      testOnUnix();
    }
  }

  @Override
  protected void testOnWindows() throws IOException {
    final String[] output = {
      "The file name must end with \"csv\" or \"txt\"!",
    };
    ProcessBuilder builder =
        new ProcessBuilder(
            "cmd.exe",
            "/c",
            toolsPath + File.separator + "import-csv.bat",
            "-h",
            ip,
            "-p",
            port,
            "-u",
            "root",
            "-pw",
            "root",
            "-f",
            "./",
            "&",
            "exit",
            "%^errorlevel%");
    builder.environment().put("CLASSPATH", libPath);
    testOutput(builder, output, 0);
  }

  @Override
  protected void testOnUnix() throws IOException {
    final String[] output = {
      "The file name must end with \"csv\" or \"txt\"!",
    };
    ProcessBuilder builder =
        new ProcessBuilder(
            "bash",
            toolsPath + File.separator + "import-csv.sh",
            "-h",
            ip,
            "-p",
            port,
            "-u",
            "root",
            "-pw",
            "root",
            "-f",
            "./");
    builder.environment().put("CLASSPATH", libPath);
    testOutput(builder, output, 0);
  }
}
