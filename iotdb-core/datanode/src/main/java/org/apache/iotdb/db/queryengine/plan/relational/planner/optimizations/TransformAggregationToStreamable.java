/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.DataOrganizationSpecification;
import org.apache.iotdb.db.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.AggregationTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.DeviceTableScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FillNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.MergeSortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.SortNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.TableFunctionProcessorNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ValueFillNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.ATTRIBUTE;
import static org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory.TAG;

/**
 * <b>Optimization phase:</b> Logical plan planning.
 *
 * <p>Calculate the preGroupedSymbols in {@link AggregationNode}, used in {@link
 * AggregationNode#isStreamable()}.
 *
 * <p>Attention: This optimizer should be used before optimizer of {@link
 * PushAggregationIntoTableScan}.
 */
public class TransformAggregationToStreamable implements PlanOptimizer {

  @Override
  public PlanNode optimize(PlanNode plan, PlanOptimizer.Context context) {
    if (!context.getAnalysis().isQuery() || !context.getAnalysis().containsAggregationQuery()) {
      return plan;
    }

    return plan.accept(new Rewriter(), null);
  }

  private static class Rewriter extends PlanVisitor<PlanNode, Void> {

    @Override
    public PlanNode visitPlan(PlanNode node, Void context) {
      for (PlanNode child : node.getChildren()) {
        child.accept(this, context);
      }
      return node;
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, Void context) {
      node.getChild().accept(this, context);
      Set<Symbol> expectedGroupingKeys = ImmutableSet.copyOf(node.getGroupingKeys());
      node.setPreGroupedSymbols(
          node.getChild()
              .accept(new DeriveGroupProperties(), new GroupContext(expectedGroupingKeys)));
      return node;
    }

    @Override
    public PlanNode visitAggregationTableScan(AggregationTableScanNode node, Void context) {
      throw new RuntimeException(
          "This optimizer should be used before optimizer of PushAggregationIntoTableScan");
    }
  }

  /**
   * This visitor returns the list of preGroupedSymbols of current AggregationNode. Attention: The
   * preGroupedSymbols of child-AggregationNode should have been calculated. GroupContext: The
   * GroupingKeys of current AggregationNode.
   */
  private static class DeriveGroupProperties extends PlanVisitor<List<Symbol>, GroupContext> {

    @Override
    public List<Symbol> visitPlan(PlanNode node, GroupContext context) {
      List<List<Symbol>> result =
          node.getChildren().stream()
              .map(child -> child.accept(new DeriveGroupProperties(), context))
              .distinct()
              .collect(Collectors.toList());
      return result.size() == 1 ? result.get(0) : ImmutableList.of();
    }

    @Override
    public List<Symbol> visitMergeSort(MergeSortNode node, GroupContext context) {
      return getMatchedPrefixSymbols(context, node.getOrderingScheme());
    }

    private List<Symbol> getMatchedPrefixSymbols(
        GroupContext context, OrderingScheme orderingScheme) {
      Set<Symbol> expectedGroupingKeys = context.groupingKeys;
      List<Symbol> orderKeys = orderingScheme.getOrderBy();
      for (int i = 0; i < orderKeys.size(); i++) {
        if (!expectedGroupingKeys.contains(orderKeys.get(i))) {
          return orderKeys.subList(0, i);
        }
      }
      return ImmutableList.of();
    }

    @Override
    public List<Symbol> visitProject(ProjectNode node, GroupContext context) {
      if (ImmutableSet.copyOf(node.getOutputSymbols()).containsAll(context.groupingKeys)) {
        return node.getChild().accept(this, context);
      }
      return ImmutableList.of();
    }

    @Override
    public List<Symbol> visitFill(FillNode node, GroupContext context) {
      if (node instanceof ValueFillNode) {
        return ImmutableList.of();
      }
      return node.getChild().accept(this, context);
    }

    @Override
    public List<Symbol> visitSort(SortNode node, GroupContext context) {
      return getMatchedPrefixSymbols(context, node.getOrderingScheme());
    }

    @Override
    public List<Symbol> visitTableFunctionProcessor(
        TableFunctionProcessorNode node, GroupContext context) {
      if (node.getChildren().isEmpty()) {
        return ImmutableList.of();
      } else if (node.isRowSemantic()) {
        return visitPlan(node, context);
      }
      Optional<DataOrganizationSpecification> dataOrganizationSpecification =
          node.getDataOrganizationSpecification();
      return dataOrganizationSpecification
          .<List<Symbol>>map(
              organizationSpecification ->
                  organizationSpecification.getPartitionBy().stream()
                      .filter(context.groupingKeys::contains)
                      .collect(Collectors.toList()))
          .orElseGet(ImmutableList::of);
    }

    @Override
    public List<Symbol> visitDeviceTableScan(DeviceTableScanNode node, GroupContext context) {
      Set<Symbol> expectedGroupingKeys = context.groupingKeys;
      Map<Symbol, ColumnSchema> assignments = node.getAssignments();
      return expectedGroupingKeys.stream()
          .filter(
              k -> {
                ColumnSchema columnSchema = assignments.get(k);
                if (columnSchema != null) {
                  return columnSchema.getColumnCategory() == TAG
                      || columnSchema.getColumnCategory() == ATTRIBUTE;
                }
                return false;
              })
          .collect(Collectors.toList());
    }

    @Override
    public List<Symbol> visitAggregation(AggregationNode node, GroupContext context) {
      return ImmutableSet.copyOf(node.getGroupingKeys()).equals(context.groupingKeys)
          ? node.getGroupingKeys()
          : ImmutableList.of();
    }

    @Override
    public List<Symbol> visitAggregationTableScan(
        AggregationTableScanNode node, GroupContext context) {
      throw new RuntimeException(
          "This optimizer should be used before optimizer of PushAggregationIntoTableScan");
    }
  }

  private static class GroupContext {
    private final Set<Symbol> groupingKeys;

    private GroupContext(Set<Symbol> groupingKeys) {
      this.groupingKeys = groupingKeys;
    }
  }
}
