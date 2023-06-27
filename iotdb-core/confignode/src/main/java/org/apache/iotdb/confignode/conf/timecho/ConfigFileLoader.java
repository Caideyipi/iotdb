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

package org.apache.iotdb.confignode.conf.timecho;

import org.apache.iotdb.confignode.conf.ConfigNodeConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.CONFIGNODE_CONF;
import static org.apache.iotdb.confignode.conf.ConfigNodeConstant.CONFIGNODE_HOME;

public class ConfigFileLoader {
  private static final Logger logger = LoggerFactory.getLogger(ConfigFileLoader.class);

  public static Path getPropsUrl(String fileName) {
    // Check if a config-directory was specified first.
    String urlString = System.getProperty(CONFIGNODE_CONF, null);

    // If it wasn't, check if a home directory was provided (This usually contains a config)
    if (urlString == null) {
      urlString = System.getProperty(CONFIGNODE_HOME, null);

      if (urlString != null) {
        urlString = urlString + File.separatorChar + "conf" + File.separatorChar + fileName;
      } else {
        // If this too wasn't provided, try to find a default config in the root of the classpath.
        URL uri = ConfigNodeConfig.class.getResource("/" + fileName);
        if (uri != null) {
          try {
            return Paths.get(uri.toURI());
          } catch (URISyntaxException e) {
            return null;
          }
        }
        logger.warn(
            "Cannot find IOTDB_HOME or IOTDB_CONF environment variable when loading "
                + "config file {}, use default configuration",
            fileName);
        return null;
      }
    }
    return Paths.get(urlString + File.separatorChar + fileName);
  }
}
