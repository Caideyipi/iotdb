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

package com.timecho.iotdb.commons.external.io;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.Objects;

public class FileUtils {
  /** An empty array of type {@link File}. */
  public static final File[] EMPTY_FILE_ARRAY = {};

  /**
   * Returns the last modification {@link FileTime} via {@link
   * java.nio.file.Files#getLastModifiedTime(Path, LinkOption...)}.
   *
   * <p>Use this method to avoid issues with {@link File#lastModified()} like <a
   * href="https://bugs.openjdk.java.net/browse/JDK-8177809">JDK-8177809</a> where {@link
   * File#lastModified()} is losing milliseconds (always ends in 000). This bug exists in OpenJDK 8
   * and 9, and is fixed in 10.
   *
   * @param file The File to query.
   * @return See {@link java.nio.file.Files#getLastModifiedTime(Path, LinkOption...)}.
   * @throws IOException if an I/O error occurs.
   * @since 2.12.0
   */
  public static FileTime lastModifiedFileTime(final File file) throws IOException {
    // https://bugs.openjdk.java.net/browse/JDK-8177809
    // File.lastModified() is losing milliseconds (always ends in 000)
    // This bug is in OpenJDK 8 and 9, and fixed in 10.
    return Files.getLastModifiedTime(Objects.requireNonNull(file.toPath(), "file"));
  }
}
