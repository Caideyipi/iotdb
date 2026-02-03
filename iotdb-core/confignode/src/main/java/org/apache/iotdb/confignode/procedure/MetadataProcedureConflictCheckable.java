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

package org.apache.iotdb.confignode.procedure;

import org.apache.iotdb.commons.path.PathPatternTree;

/**
 * Interface for metadata procedures that can check conflicts based on path patterns.
 *
 * <p>This interface is used to check conflicts between metadata procedures that operate on
 * tree-model metadata. Procedures implementing this interface can apply their path patterns to a
 * given PathPatternTree, which will be used to check for conflicts with other running procedures.
 */
public interface MetadataProcedureConflictCheckable {

  /**
   * Apply path patterns from this procedure to the given pattern tree.
   *
   * <p>This method should add all paths that this procedure operates on to the provided
   * PathPatternTree. The procedure is responsible for building and applying its own path patterns
   * to the tree.
   *
   * @param patternTree the pattern tree to apply path patterns to
   */
  void applyPathPatterns(PathPatternTree patternTree);

  /**
   * Check if this procedure conflicts with the given pattern tree.
   *
   * <p>This method checks if the paths operated by this procedure overlap with the paths in the
   * given pattern tree. Returns true if there is no conflict, false if there is a conflict.
   *
   * @param patternTree the pattern tree to check against
   * @return true if there is no conflict (can proceed), false if there is a conflict (should throw
   *     exception)
   */
  default boolean hasNoConflictWith(PathPatternTree patternTree) {
    if (patternTree == null || patternTree.isEmpty()) {
      return true; // No conflict if the pattern tree is empty
    }
    // Build this procedure's pattern tree
    PathPatternTree thisProcedureTree = new PathPatternTree();
    applyPathPatterns(thisProcedureTree);
    thisProcedureTree.constructTree();
    if (thisProcedureTree.isEmpty()) {
      return true;
    }
    // Check if there's overlap
    return !thisProcedureTree.isOverlapWith(patternTree);
  }

  /**
   * Check if this procedure should be included in conflict checking.
   *
   * <p>This method can be used to filter out procedures that are not relevant for conflict
   * checking, such as finished procedures or procedures that don't operate on paths.
   *
   * @return true if this procedure should be included in conflict checking, false otherwise
   */
  default boolean shouldCheckConflict() {
    return true;
  }
}
