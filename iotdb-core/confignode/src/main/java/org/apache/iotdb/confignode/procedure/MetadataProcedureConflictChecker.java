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

import java.util.Collection;

/**
 * Utility class for checking conflicts between metadata procedures based on path patterns.
 *
 * <p>This class provides unified methods to check conflicts for tree-model metadata procedures:
 *
 * <ul>
 *   <li>Build a PathPatternTree from all running metadata procedures
 * </ul>
 */
public class MetadataProcedureConflictChecker {

  private MetadataProcedureConflictChecker() {
    // Utility class, prevent instantiation
  }

  /**
   * Build a PathPatternTree containing all path patterns from running metadata procedures.
   *
   * <p>This method iterates through all running procedures and extracts their path patterns using
   * the MetadataProcedureConflictCheckable interface. Procedures that implement this interface will
   * apply their paths to the tree for conflict checking.
   *
   * @param runningProcedures collection of running procedures
   * @return a PathPatternTree containing all path patterns from running metadata procedures
   */
  public static PathPatternTree buildRunningTasksPatternTree(
      Collection<? extends Procedure<?>> runningProcedures) {
    PathPatternTree patternTree = new PathPatternTree();

    for (Procedure<?> runningProcedure : runningProcedures) {
      // Skip if procedure is finished
      if (runningProcedure.isFinished()) {
        continue;
      }

      // Use MetadataProcedureConflictCheckable interface - no need for type checking
      if (runningProcedure instanceof MetadataProcedureConflictCheckable) {
        MetadataProcedureConflictCheckable checkable =
            (MetadataProcedureConflictCheckable) runningProcedure;
        if (checkable.shouldCheckConflict()) {
          // Let the procedure apply its path patterns to the tree
          checkable.applyPathPatterns(patternTree);
        }
      }
    }

    patternTree.constructTree();
    return patternTree;
  }
}
