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

package org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.rule;

import org.apache.iotdb.db.queryengine.plan.relational.planner.Assignments;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.planner.SymbolsExtractor;
import org.apache.iotdb.db.queryengine.plan.relational.planner.iterative.Rule;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.Measure;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.PatternRecognitionNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.ExpressionAndValuePointers;
import org.apache.iotdb.db.queryengine.plan.relational.planner.rowpattern.IrLabel;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Captures;
import org.apache.iotdb.db.queryengine.plan.relational.utils.matching.Pattern;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.filter;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.patternRecognition;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.Patterns.project;

public class ExpressionRewriteRuleSet {

  public interface ExpressionRewriter {
    Expression rewrite(Expression expression, Rule.Context context);
  }

  private final ExpressionRewriter rewriter;

  public ExpressionRewriteRuleSet(ExpressionRewriter rewriter) {
    this.rewriter = requireNonNull(rewriter, "rewriter is null");
  }

  public Set<Rule<?>> rules() {
    return ImmutableSet.of(
        projectExpressionRewrite(),
        //        aggregationExpressionRewrite(),
        filterExpressionRewrite(),
        //        joinExpressionRewrite(),
        //        valuesExpressionRewrite(),
        patternRecognitionExpressionRewrite());
  }

  public Rule<?> projectExpressionRewrite() {
    return new ProjectExpressionRewrite(rewriter);
  }

  // TODO add it back after we support AggregationNode

  //  public Rule<?> aggregationExpressionRewrite() {
  //    return new AggregationExpressionRewrite(rewriter);
  //  }

  public Rule<?> filterExpressionRewrite() {
    return new FilterExpressionRewrite(rewriter);
  }

  // TODO add it back after we support JoinNode
  //  public Rule<?> joinExpressionRewrite() {
  //    return new JoinExpressionRewrite(rewriter);
  //  }

  // TODO add it back after we support ValuesNode
  //  public Rule<?> valuesExpressionRewrite() {
  //    return new ValuesExpressionRewrite(rewriter);
  //  }

  public Rule<?> patternRecognitionExpressionRewrite() {
    return new PatternRecognitionExpressionRewrite(rewriter);
  }

  private static final class ProjectExpressionRewrite implements Rule<ProjectNode> {
    private final ExpressionRewriter rewriter;

    ProjectExpressionRewrite(ExpressionRewriter rewriter) {
      this.rewriter = rewriter;
    }

    @Override
    public Pattern<ProjectNode> getPattern() {
      return project();
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context) {
      Assignments assignments =
          projectNode.getAssignments().rewrite(x -> rewriter.rewrite(x, context));
      if (projectNode.getAssignments().equals(assignments)) {
        return Result.empty();
      }
      return Result.ofPlanNode(
          new ProjectNode(projectNode.getPlanNodeId(), projectNode.getChild(), assignments));
    }

    @Override
    public String toString() {
      return format("%s(%s)", getClass().getSimpleName(), rewriter);
    }
  }

  // TODO add it back after we support AggregationNode

  //  private static final class AggregationExpressionRewrite
  //      implements Rule<AggregationNode>
  //  {
  //    private final ExpressionRewriter rewriter;
  //
  //    AggregationExpressionRewrite(ExpressionRewriter rewriter)
  //    {
  //      this.rewriter = rewriter;
  //    }
  //
  //    @Override
  //    public Pattern<AggregationNode> getPattern()
  //    {
  //      return aggregation();
  //    }
  //
  //    @Override
  //    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
  //    {
  //      boolean anyRewritten = false;
  //      ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
  //      for (Map.Entry<Symbol, Aggregation> entry : aggregationNode.getAggregations().entrySet())
  // {
  //        Aggregation aggregation = entry.getValue();
  //        Aggregation newAggregation = new Aggregation(
  //            aggregation.getResolvedFunction(),
  //            aggregation.getArguments().stream()
  //                .map(argument -> rewriter.rewrite(argument, context))
  //                .collect(toImmutableList()),
  //            aggregation.isDistinct(),
  //            aggregation.getFilter(),
  //            aggregation.getOrderingScheme(),
  //            aggregation.getMask());
  //        aggregations.put(entry.getKey(), newAggregation);
  //        if (!aggregation.equals(newAggregation)) {
  //          anyRewritten = true;
  //        }
  //      }
  //      if (anyRewritten) {
  //        return Result.ofPlanNode(AggregationNode.builderFrom(aggregationNode)
  //            .setAggregations(aggregations.buildOrThrow())
  //            .build());
  //      }
  //      return Result.empty();
  //    }
  //
  //    @Override
  //    public String toString()
  //    {
  //      return format("%s(%s)", getClass().getSimpleName(), rewriter);
  //    }
  //  }

  private static final class FilterExpressionRewrite implements Rule<FilterNode> {
    private final ExpressionRewriter rewriter;

    FilterExpressionRewrite(ExpressionRewriter rewriter) {
      this.rewriter = rewriter;
    }

    @Override
    public Pattern<FilterNode> getPattern() {
      return filter();
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context) {
      Expression rewritten = rewriter.rewrite(filterNode.getPredicate(), context);
      if (filterNode.getPredicate().equals(rewritten)) {
        return Result.empty();
      }
      return Result.ofPlanNode(
          new FilterNode(filterNode.getPlanNodeId(), filterNode.getChild(), rewritten));
    }

    @Override
    public String toString() {
      return format("%s(%s)", getClass().getSimpleName(), rewriter);
    }
  }

  // TODO add it back after we support JoinNode

  //  private static final class JoinExpressionRewrite
  //      implements Rule<JoinNode>
  //  {
  //    private final ExpressionRewriter rewriter;
  //
  //    JoinExpressionRewrite(ExpressionRewriter rewriter)
  //    {
  //      this.rewriter = rewriter;
  //    }
  //
  //    @Override
  //    public Pattern<JoinNode> getPattern()
  //    {
  //      return join();
  //    }
  //
  //    @Override
  //    public Result apply(JoinNode joinNode, Captures captures, Context context)
  //    {
  //      Optional<Expression> filter = joinNode.getFilter().map(x -> rewriter.rewrite(x, context));
  //      if (!joinNode.getFilter().equals(filter)) {
  //        return Result.ofPlanNode(new JoinNode(
  //            joinNode.getId(),
  //            joinNode.getType(),
  //            joinNode.getLeft(),
  //            joinNode.getRight(),
  //            joinNode.getCriteria(),
  //            joinNode.getLeftOutputSymbols(),
  //            joinNode.getRightOutputSymbols(),
  //            joinNode.isMaySkipOutputDuplicates(),
  //            filter,
  //            joinNode.getLeftHashSymbol(),
  //            joinNode.getRightHashSymbol(),
  //            joinNode.getDistributionType(),
  //            joinNode.isSpillable(),
  //            joinNode.getDynamicFilters(),
  //            joinNode.getReorderJoinStatsAndCost()));
  //      }
  //      return Result.empty();
  //    }
  //
  //    @Override
  //    public String toString()
  //    {
  //      return format("%s(%s)", getClass().getSimpleName(), rewriter);
  //    }
  //  }

  // TODO add it back after we support ValuesNode

  //  private static final class ValuesExpressionRewrite
  //      implements Rule<ValuesNode>
  //  {
  //    private final ExpressionRewriter rewriter;
  //
  //    ValuesExpressionRewrite(ExpressionRewriter rewriter)
  //    {
  //      this.rewriter = rewriter;
  //    }
  //
  //    @Override
  //    public Pattern<ValuesNode> getPattern()
  //    {
  //      return values();
  //    }
  //
  //    @Override
  //    public Result apply(ValuesNode valuesNode, Captures captures, Context context)
  //    {
  //      if (valuesNode.getRows().isEmpty()) {
  //        return Result.empty();
  //      }
  //
  //      boolean anyRewritten = false;
  //      ImmutableList.Builder<Expression> rows = ImmutableList.builder();
  //      for (Expression row : valuesNode.getRows().get()) {
  //        Expression rewritten;
  //        if (row instanceof Row) {
  //          // preserve the structure of row
  //          rewritten = new Row(((Row) row).getItems().stream()
  //              .map(item -> rewriter.rewrite(item, context))
  //              .collect(toImmutableList()));
  //        }
  //        else {
  //          rewritten = rewriter.rewrite(row, context);
  //        }
  //        if (!row.equals(rewritten)) {
  //          anyRewritten = true;
  //        }
  //        rows.add(rewritten);
  //      }
  //      if (anyRewritten) {
  //        return Result.ofPlanNode(new ValuesNode(valuesNode.getId(),
  // valuesNode.getOutputSymbols(), rows.build()));
  //      }
  //      return Result.empty();
  //    }
  //
  //    @Override
  //    public String toString()
  //    {
  //      return format("%s(%s)", getClass().getSimpleName(), rewriter);
  //    }
  //  }

  private static final class PatternRecognitionExpressionRewrite
      implements Rule<PatternRecognitionNode> {
    private final ExpressionRewriter rewriter;

    PatternRecognitionExpressionRewrite(ExpressionRewriter rewriter) {
      this.rewriter = rewriter;
    }

    @Override
    public Pattern<PatternRecognitionNode> getPattern() {
      return patternRecognition();
    }

    @Override
    public Result apply(PatternRecognitionNode node, Captures captures, Context context) {
      boolean anyRewritten = false;

      // rewrite MEASURES expressions
      ImmutableMap.Builder<Symbol, Measure> rewrittenMeasures = ImmutableMap.builder();
      for (Map.Entry<Symbol, Measure> entry : node.getMeasures().entrySet()) {
        ExpressionAndValuePointers pointers = entry.getValue().getExpressionAndValuePointers();
        Optional<ExpressionAndValuePointers> newPointers = rewrite(pointers, context);
        if (newPointers.isPresent()) {
          anyRewritten = true;
          rewrittenMeasures.put(
              entry.getKey(), new Measure(newPointers.get(), entry.getValue().getType()));
        } else {
          rewrittenMeasures.put(entry);
        }
      }

      // rewrite DEFINE expressions
      ImmutableMap.Builder<IrLabel, ExpressionAndValuePointers> rewrittenDefinitions =
          ImmutableMap.builder();
      for (Map.Entry<IrLabel, ExpressionAndValuePointers> entry :
          node.getVariableDefinitions().entrySet()) {
        ExpressionAndValuePointers pointers = entry.getValue();
        Optional<ExpressionAndValuePointers> newPointers = rewrite(pointers, context);
        if (newPointers.isPresent()) {
          anyRewritten = true;
          rewrittenDefinitions.put(entry.getKey(), newPointers.get());
        } else {
          rewrittenDefinitions.put(entry);
        }
      }

      if (anyRewritten) {
        return Result.ofPlanNode(
            new PatternRecognitionNode(
                node.getPlanNodeId(),
                node.getChild(),
                node.getPartitionBy(),
                node.getOrderingScheme(),
                node.getHashSymbol(),
                rewrittenMeasures.buildOrThrow(),
                node.getRowsPerMatch(),
                node.getSkipToLabels(),
                node.getSkipToPosition(),
                node.getPattern(),
                rewrittenDefinitions.buildOrThrow()));
      }

      return Result.empty();
    }

    // return Optional containing the rewritten ExpressionAndValuePointers, or Optional.empty() in
    // case when no rewrite applies
    private Optional<ExpressionAndValuePointers> rewrite(
        ExpressionAndValuePointers pointers, Context context) {
      boolean rewritten = false;

      // rewrite top-level expression
      Expression newExpression = rewriter.rewrite(pointers.getExpression(), context);
      if (!pointers.getExpression().equals(newExpression)) {
        rewritten = true;
      }

      // prune unused symbols
      ImmutableList.Builder<ExpressionAndValuePointers.Assignment> newAssignments =
          ImmutableList.builder();

      Set<Symbol> newSymbols = SymbolsExtractor.extractUnique(newExpression);
      for (ExpressionAndValuePointers.Assignment assignment : pointers.getAssignments()) {
        if (newSymbols.contains(assignment.getSymbol())) {
          newAssignments.add(
              new ExpressionAndValuePointers.Assignment(
                  assignment.getSymbol(), assignment.getValuePointer()));
        }
      }

      if (rewritten) {
        return Optional.of(new ExpressionAndValuePointers(newExpression, newAssignments.build()));
      }

      return Optional.empty();
    }

    @Override
    public String toString() {
      return format("%s(%s)", getClass().getSimpleName(), rewriter);
    }
  }
}
