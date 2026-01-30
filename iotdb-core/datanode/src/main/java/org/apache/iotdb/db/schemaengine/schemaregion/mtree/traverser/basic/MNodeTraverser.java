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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.basic;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeIterator;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.IMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.traverser.Traverser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class defines any node in MTree as potential target node. On finding a path matching the
 * given pattern, if a level is specified and the path is longer than the specified level,
 * MNodeTraverser finds the node of the specified level on the path and process it. The same node
 * will not be processed more than once. If a level is not given, the current node is processed.
 */
public abstract class MNodeTraverser<R, N extends IMNode<N>> extends Traverser<R, N> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MNodeTraverser.class);

  // Level query option started from 0. For example, level of root.sg.d1.s1 is 3.
  protected int targetLevel = -1;
  protected N lastVisitNode = null;

  /**
   * To traverse subtree under root.sg, e.g., init Traverser(root, "root.sg.**")
   *
   * @param startNode denote which tree to traverse by passing its root
   * @param path use wildcard to specify which part to traverse
   * @param store MTree store to traverse
   * @param isPrefixMatch prefix match or not
   * @param scope traversing scope
   * @throws MetadataException path does not meet the expected rules
   */
  public MNodeTraverser(
      N startNode,
      PartialPath path,
      IMTreeStore<N> store,
      boolean isPrefixMatch,
      PathPatternTree scope)
      throws MetadataException {
    super(startNode, path, store, isPrefixMatch, scope);
  }

  @Override
  protected boolean mayTargetNodeType(N node) {
    return true;
  }

  @Override
  protected boolean acceptFullyMatchedNode(N node) {
    boolean shouldAccept;
    N targetNode;

    if (targetLevel >= 0) {
      if (getSizeOfAncestor() > targetLevel) {
        targetNode = getAncestorNodeByLevel(targetLevel);
        shouldAccept = targetNode != lastVisitNode;
      } else if (getSizeOfAncestor() == targetLevel) {
        targetNode = node;
        shouldAccept = node != lastVisitNode;
      } else {
        return false;
      }
    } else {
      targetNode = node;
      shouldAccept = true;
    }

    // If should accept, check if all children are disabled measurements
    // Only check non-measurement nodes (measurement nodes are leaf nodes with no children)
    if (shouldAccept && skipInvalidSchema && targetNode != null) {
      if (node.isMeasurement() && node.getAsMeasurementMNode().isInvalid()) {
        return false;
      }
      try {
        return !hasOnlyInvalidChildren(targetNode);
      } catch (MetadataException e) {
        // If we can't check, accept the node to be safe
        LOGGER.warn("Failed to check if node has only invalid children", e);
      }
    }

    return shouldAccept;
  }

  /**
   * Check if all descendant nodes (recursively) are invalid measurements. Returns true if: 1. The
   * node has children (at any level) 2. All descendant nodes are measurement nodes 3. All
   * measurement descendant nodes are invalid (DISABLED=true)
   *
   * <p>This method recursively checks all levels of children in the prefix tree structure. For
   * example, for paths like root.a.b.b.c and root.a.c.b.a, it checks all descendant nodes.
   *
   * <p>Note: This method should only be called for non-measurement nodes (measurement nodes are
   * leaf nodes with no children).
   *
   * @param node the node to check (must not be a measurement node)
   * @return true if all descendants are invalid measurements, false otherwise
   * @throws MetadataException if an error occurs while checking the node
   */
  private boolean hasOnlyInvalidChildren(N node) throws MetadataException {
    // Use store.getChildrenIterator directly to get all children including invalid ones
    IMNodeIterator<N> childrenIterator = store.getChildrenIterator(node);
    try {
      boolean hasChild = false;
      boolean hasEnabledChild = false;

      while (childrenIterator.hasNext()) {
        N child = childrenIterator.next();
        hasChild = true;

        // Check if child is enabled: either a non-invalid measurement or a non-measurement with
        // enabled descendants
        if ((child.isMeasurement() && !child.getAsMeasurementMNode().isInvalid())
            || (!child.isMeasurement() && !hasOnlyInvalidChildren(child))) {
          hasEnabledChild = true;
          break;
        }
      }

      // If node has children and all are disabled measurements (recursively), return true
      return hasChild && !hasEnabledChild;
    } finally {
      childrenIterator.close();
    }
  }

  @Override
  protected boolean acceptInternalMatchedNode(N node) {
    return false;
  }

  @Override
  protected boolean shouldVisitSubtreeOfFullMatchedNode(N node) {
    return !node.isMeasurement();
  }

  @Override
  protected boolean shouldVisitSubtreeOfInternalMatchedNode(N node) {
    return !node.isMeasurement();
  }

  public void setTargetLevel(int targetLevel) {
    this.targetLevel = targetLevel;
  }

  @Override
  protected final R generateResult(N nextMatchedNode) {
    if (targetLevel >= 0) {
      if (getLevelOfNextMatchedNode() == targetLevel) {
        lastVisitNode = nextMatchedNode;
      } else {
        lastVisitNode = getAncestorNodeByLevel(targetLevel);
      }
      return transferToResult(lastVisitNode);
    } else {
      return transferToResult(nextMatchedNode);
    }
  }

  protected abstract R transferToResult(N node);
}
