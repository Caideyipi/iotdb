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

package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.last.LastQueryNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SourceNode;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.crud.QueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTimeSeriesStatement;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NodeGroupContext {

  protected final MPPQueryContext queryContext;
  private final Map<PlanNodeId, NodeDistribution> nodeDistributionMap;
  private boolean isAlignByDevice;
  private TRegionReplicaSet mostlyUsedDataRegion;
  protected boolean hasExchangeNode;

  public NodeGroupContext(MPPQueryContext queryContext, Statement statement, PlanNode root) {
    this.queryContext = queryContext;
    this.nodeDistributionMap = new HashMap<>();
    if (statement instanceof QueryStatement) {
      this.isAlignByDevice = ((QueryStatement) statement).isAlignByDevice();
      this.mostlyUsedDataRegion = getMostlyUsedDataRegion(root);
    } else if (statement instanceof ShowTimeSeriesStatement) {
      this.mostlyUsedDataRegion = getMostlyUsedDataRegion(root);
    }
    this.hasExchangeNode = false;
  }

  private TRegionReplicaSet getMostlyUsedDataRegion(PlanNode root) {
    Map<TRegionReplicaSet, Long> regionCountMap = new HashMap<>();
    countRegionOfSourceNodes(root, regionCountMap);
    if (regionCountMap.isEmpty()) {
      return DataPartition.NOT_ASSIGNED;
    }
    return Collections.max(regionCountMap.entrySet(), Map.Entry.comparingByValue()).getKey();
  }

  private void countRegionOfSourceNodes(PlanNode root, Map<TRegionReplicaSet, Long> result) {
    if (root instanceof LastQueryNode) {
      // At this point, there should only be LastSeriesSourceNode in LastQueryNode, and all of them
      // have been grouped in the rewriteSource stage by region.
      TRegionReplicaSet regionReplicaSet = ((LastQueryNode) root).getRegionReplicaSetByFirstChild();
      if (regionReplicaSet != DataPartition.NOT_ASSIGNED) {
        result.compute(
            regionReplicaSet,
            (region, count) -> (count == null) ? 1 : count + root.getChildren().size());
      }
      return;
    }
    root.getChildren().forEach(child -> countRegionOfSourceNodes(child, result));
    if (root instanceof SourceNode) {
      TRegionReplicaSet regionReplicaSet = ((SourceNode) root).getRegionReplicaSet();
      if (regionReplicaSet != DataPartition.NOT_ASSIGNED) {
        result.compute(regionReplicaSet, (region, count) -> (count == null) ? 1 : count + 1);
      }
    }
  }

  public void putNodeDistribution(PlanNodeId nodeId, NodeDistribution distribution) {
    this.nodeDistributionMap.put(nodeId, distribution);
  }

  public NodeDistribution getNodeDistribution(PlanNodeId nodeId) {
    return this.nodeDistributionMap.get(nodeId);
  }

  public boolean isAlignByDevice() {
    return isAlignByDevice;
  }

  public TRegionReplicaSet getMostlyUsedDataRegion() {
    return mostlyUsedDataRegion;
  }
}
