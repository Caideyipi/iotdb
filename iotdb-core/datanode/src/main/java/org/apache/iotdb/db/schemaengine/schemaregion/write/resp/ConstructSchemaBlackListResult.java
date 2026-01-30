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
 * software distributed under this work for additional information
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

package org.apache.iotdb.db.schemaengine.schemaregion.write.resp;

import org.apache.iotdb.commons.path.PartialPath;

import org.apache.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Result of constructSchemaBlackList operation, containing: 1. Pre-deleted series count and whether
 * all are logical views (original {@code Pair<Long, Boolean>}) 2. Whether all matched series are
 * invalid series 3. Referenced invalid paths information list (associated invalid series that need
 * to be deleted but don't need pre-delete)
 */
public class ConstructSchemaBlackListResult {

  private final long preDeletedNum;
  private final boolean isAllLogicalView;
  private final boolean isAllInvalidSeries;
  private final List<ReferencedInvalidPathInfo> referencedInvalidPaths;
  private final List<PartialPath> preDeletedPaths; // Active deletion paths (non-invalid series)
  private final boolean hasInvalidSeries; // Whether there are any invalid series

  public ConstructSchemaBlackListResult(
      final long preDeletedNum,
      final boolean isAllLogicalView,
      final boolean isAllInvalidSeries,
      final boolean hasInvalidSeries,
      final List<ReferencedInvalidPathInfo> referencedInvalidPaths,
      final List<PartialPath> preDeletedPaths) {
    this.preDeletedNum = preDeletedNum;
    this.isAllLogicalView = isAllLogicalView;
    this.isAllInvalidSeries = isAllInvalidSeries;
    this.referencedInvalidPaths =
        referencedInvalidPaths != null ? referencedInvalidPaths : new ArrayList<>();
    this.preDeletedPaths = preDeletedPaths != null ? preDeletedPaths : new ArrayList<>();
    this.hasInvalidSeries = hasInvalidSeries;
  }

  public ConstructSchemaBlackListResult(final Pair<Long, Boolean> pair) {
    this.preDeletedNum = pair.left;
    this.isAllLogicalView = pair.right;
    this.isAllInvalidSeries = false;
    this.referencedInvalidPaths = new ArrayList<>();
    this.preDeletedPaths = new ArrayList<>();
    this.hasInvalidSeries = false;
  }

  public long getPreDeletedNum() {
    return preDeletedNum;
  }

  public boolean isAllLogicalView() {
    return isAllLogicalView;
  }

  public boolean isAllInvalidSeries() {
    return isAllInvalidSeries;
  }

  public List<ReferencedInvalidPathInfo> getReferencedInvalidPaths() {
    return referencedInvalidPaths;
  }

  public List<PartialPath> getPreDeletedPaths() {
    return preDeletedPaths;
  }

  public boolean hasInvalidSeries() {
    return hasInvalidSeries;
  }

  public Pair<Long, Boolean> toPair() {
    return new Pair<>(preDeletedNum, isAllLogicalView);
  }

  /**
   * Information about a referenced invalid path that needs to be deleted. Referenced invalid paths
   * are associated with the series being deleted (e.g., if deleting an alias series, its
   * corresponding invalid physical series should also be deleted). Invalid series don't need
   * pre-delete because they are already invalid.
   */
  public static class ReferencedInvalidPathInfo {
    private final PartialPath path; // The invalid series path
    private final PartialPath originalPath; // ORIGINAL_PATH if this is an alias series
    private final boolean isRenamed; // IS_RENAMED flag

    public ReferencedInvalidPathInfo(
        final PartialPath path, final PartialPath originalPath, final boolean isRenamed) {
      this.path = path;
      this.originalPath = originalPath;
      this.isRenamed = isRenamed;
    }

    public PartialPath getPath() {
      return path;
    }

    public PartialPath getOriginalPath() {
      return originalPath;
    }

    public boolean isRenamed() {
      return isRenamed;
    }
  }
}
