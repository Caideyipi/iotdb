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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.commons.partition.SchemaPartition;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.execution.warnings.WarningCollector;
import org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.planner.plan.LogicalQueryPlan;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.WritePlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.CountSchemaMergeNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedWritePlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Analysis;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationId;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.RelationType;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Scope;
import org.apache.iotdb.db.queryengine.plan.relational.execution.querystats.PlanOptimizersStatsCollector;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableMetadataImpl;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ExplainAnalyzeNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.FilterNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.IntoNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.LimitNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OffsetNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.ProjectNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeUpdateNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceFetchNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryCountNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceQueryScanNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.LogicalOptimizeFactory;
import org.apache.iotdb.db.queryengine.plan.relational.planner.optimizations.PlanOptimizer;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractQueryDeviceWithCache;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AbstractTraverseDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateOrUpdateDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Delete;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Explain;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExplainAnalyze;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Insert;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.PipeEnriched;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Query;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Table;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Update;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WrappedStatement;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.LongType;
import org.apache.tsfile.read.common.type.StringType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.LOGICAL_PLANNER;
import static org.apache.iotdb.db.queryengine.metric.QueryPlanCostMetricSet.LOGICAL_PLAN_OPTIMIZE;
import static org.apache.iotdb.db.queryengine.plan.relational.metadata.MetadataUtil.createQualifiedObjectName;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.QueryPlanner.visibleFields;
import static org.apache.iotdb.db.queryengine.plan.relational.planner.node.OutputNode.COLUMN_NAME_PREFIX;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CountDevice.COUNT_DEVICE_HEADER_STRING;
import static org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowDevice.getDeviceColumnHeaderList;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;

public class TableLogicalPlanner {
  private final MPPQueryContext queryContext;
  private final SessionInfo sessionInfo;
  private final SymbolAllocator symbolAllocator;
  private final List<PlanOptimizer> planOptimizers;
  private final Metadata metadata;
  private final WarningCollector warningCollector;

  @TestOnly
  public TableLogicalPlanner(
      MPPQueryContext queryContext,
      Metadata metadata,
      SessionInfo sessionInfo,
      SymbolAllocator symbolAllocator,
      WarningCollector warningCollector) {
    this(
        queryContext,
        metadata,
        sessionInfo,
        symbolAllocator,
        warningCollector,
        new LogicalOptimizeFactory(new PlannerContext(metadata, new InternalTypeManager()))
            .getPlanOptimizers());
  }

  public TableLogicalPlanner(
      MPPQueryContext queryContext,
      Metadata metadata,
      SessionInfo sessionInfo,
      SymbolAllocator symbolAllocator,
      WarningCollector warningCollector,
      List<PlanOptimizer> planOptimizers) {
    this.queryContext = queryContext;
    this.metadata = metadata;
    this.sessionInfo = requireNonNull(sessionInfo, "session is null");
    this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
    this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    this.planOptimizers = planOptimizers;
  }

  public LogicalQueryPlan plan(final Analysis analysis) {
    long startTime = System.nanoTime();
    final Statement statement = analysis.getStatement();
    PlanNode planNode = planStatement(analysis, statement);

    if (analysis.isQuery()) {
      long logicalPlanCostTime = System.nanoTime() - startTime;
      QueryPlanCostMetricSet.getInstance()
          .recordTablePlanCost(LOGICAL_PLANNER, logicalPlanCostTime);
      queryContext.setLogicalPlanCost(logicalPlanCostTime);

      startTime = System.nanoTime();
      for (PlanOptimizer optimizer : planOptimizers) {
        planNode =
            optimizer.optimize(
                planNode,
                new PlanOptimizer.Context(
                    sessionInfo,
                    analysis,
                    metadata,
                    queryContext,
                    symbolAllocator,
                    queryContext.getQueryId(),
                    warningCollector,
                    PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector()));
      }
      long logicalOptimizationCost =
          System.nanoTime()
              - startTime
              - queryContext.getFetchPartitionCost()
              - queryContext.getFetchSchemaCost();
      queryContext.setLogicalOptimizationCost(logicalOptimizationCost);
      QueryPlanCostMetricSet.getInstance()
          .recordTablePlanCost(LOGICAL_PLAN_OPTIMIZE, logicalOptimizationCost);
    }

    return new LogicalQueryPlan(queryContext, planNode);
  }

  private PlanNode planStatement(final Analysis analysis, Statement statement) {
    // Schema statements are handled here
    if (statement instanceof PipeEnriched) {
      Statement innerStatement = ((PipeEnriched) statement).getInnerStatement();
      if (innerStatement instanceof CreateOrUpdateDevice) {
        return new PipeEnrichedWritePlanNode(
            (WritePlanNode)
                planCreateOrUpdateDevice((CreateOrUpdateDevice) innerStatement, analysis));
      }
      if (innerStatement instanceof Update) {
        return new PipeEnrichedWritePlanNode(
            (WritePlanNode) planUpdate((Update) innerStatement, analysis));
      }
    }
    if (statement instanceof CreateOrUpdateDevice) {
      return planCreateOrUpdateDevice((CreateOrUpdateDevice) statement, analysis);
    }
    if (statement instanceof FetchDevice) {
      return planFetchDevice((FetchDevice) statement, analysis);
    }
    if (statement instanceof ShowDevice) {
      return planShowDevice((ShowDevice) statement, analysis);
    }
    if (statement instanceof CountDevice) {
      return planCountDevice((CountDevice) statement, analysis);
    }
    if (statement instanceof Update) {
      return planUpdate((Update) statement, analysis);
    }
    return createOutputPlan(planStatementWithoutOutput(analysis, statement), analysis);
  }

  private RelationPlan planStatementWithoutOutput(Analysis analysis, Statement statement) {
    if (statement instanceof Query) {
      return createRelationPlan(analysis, (Query) statement);
    }
    if (statement instanceof Explain) {
      return createRelationPlan(analysis, (Query) ((Explain) statement).getStatement());
    }
    if (statement instanceof WrappedStatement) {
      return createRelationPlan(analysis, ((WrappedStatement) statement));
    }
    if (statement instanceof LoadTsFile) {
      return createRelationPlan(analysis, (LoadTsFile) statement);
    }
    if (statement instanceof PipeEnriched) {
      return createRelationPlan(analysis, (PipeEnriched) statement);
    }
    if (statement instanceof Delete) {
      return createRelationPlan(analysis, (Delete) statement);
    }
    if (statement instanceof ExplainAnalyze) {
      return planExplainAnalyze((ExplainAnalyze) statement, analysis);
    }
    if (statement instanceof Insert) {
      return genInsertPlan(analysis, (Insert) statement);
    }
    throw new IllegalStateException(
        "Unsupported statement type: " + statement.getClass().getSimpleName());
  }

  private RelationPlan genInsertPlan(final Analysis analysis, final Insert node) {
    // query plan and visible fields
    Query query = node.getQuery();
    RelationPlan plan = createRelationPlan(analysis, query);
    List<Symbol> visibleFieldMappings = visibleFields(plan);

    // table columns
    Table table = node.getTable();
    QualifiedObjectName targetTable = createQualifiedObjectName(sessionInfo, table.getName());
    Optional<TableSchema> tableSchema = metadata.getTableSchema(sessionInfo, targetTable);
    if (!tableSchema.isPresent()) {
      TableMetadataImpl.throwTableNotExistsException(
          targetTable.getDatabaseName(), targetTable.getObjectName());
    }
    List<ColumnSchema> tableColumns = tableSchema.get().getColumns();
    Map<String, ColumnSchema> columnSchemaMap = tableSchema.get().getColumnSchemaMap();

    // insert columns
    Analysis.Insert insert = analysis.getInsert();
    List<ColumnSchema> insertColumns = insert.getColumns();

    // prepare Assignments and ColumnSchema builder
    Assignments.Builder assignments = Assignments.builder();
    ImmutableList.Builder<ColumnSchema> insertedColumnsBuilder = ImmutableList.builder();

    // insert null if table column is not in query columns.
    for (ColumnSchema column : tableColumns) {
      if (column.isHidden()) {
        continue;
      }
      Symbol output = symbolAllocator.newSymbol(column.getName(), column.getType());
      Expression expression;
      Type tableType = column.getType();
      int index = insertColumns.indexOf(columnSchemaMap.get(column.getName()));
      if (index < 0) {
        expression = new NullLiteral();
      } else {
        Symbol input = visibleFieldMappings.get(index);
        Type queryType = symbolAllocator.getTypes().getTableModelType(input);
        if (!queryType.equals(tableType)) {
          throw new SemanticException(
              String.format(
                  "Insert query has mismatched column type: Table: [%s], Query: [%s]",
                  tableType, queryType));
        }
        expression = input.toSymbolReference();
      }
      assignments.put(output, expression);
      insertedColumnsBuilder.add(column);
    }

    // Project Node
    ProjectNode projectNode =
        new ProjectNode(
            queryContext.getQueryId().genPlanNodeId(), plan.getRoot(), assignments.build());
    List<ColumnSchema> insertedColumns = insertedColumnsBuilder.build();
    List<Field> fields =
        insertedColumns.stream()
            .map(
                column ->
                    Field.newUnqualified(
                        column.getName(), column.getType(), column.getColumnCategory()))
            .collect(toImmutableList());
    Scope scope =
        Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields)).build();
    plan = new RelationPlan(projectNode, scope, projectNode.getOutputSymbols(), Optional.empty());

    // Into Node
    IntoNode intoNode =
        new IntoNode(
            queryContext.getQueryId().genPlanNodeId(),
            plan.getRoot(),
            targetTable.getDatabaseName(),
            table.getName().getSuffix(),
            tableColumns,
            symbolAllocator.newSymbol(Insert.ROWS, Insert.ROWS_TYPE));
    return new RelationPlan(
        intoNode, analysis.getRootScope(), intoNode.getOutputSymbols(), Optional.empty());
  }

  private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis) {
    if (plan.getRoot() instanceof WritePlanNode) {
      return plan.getRoot();
    }
    ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
    ImmutableList.Builder<String> names = ImmutableList.builder();
    List<ColumnHeader> columnHeaders = new ArrayList<>();

    int columnNumber = 0;
    // TODO perfect the logic of outputDescriptor
    if (queryContext.isExplainAnalyze()) {
      outputs.add(new Symbol(ColumnHeaderConstant.EXPLAIN_ANALYZE));
      names.add(ColumnHeaderConstant.EXPLAIN_ANALYZE);
      columnHeaders.add(new ColumnHeader(ColumnHeaderConstant.EXPLAIN_ANALYZE, TSDataType.TEXT));
    } else {
      RelationType outputDescriptor = analysis.getOutputDescriptor();
      for (Field field : outputDescriptor.getVisibleFields()) {
        String name = field.getName().orElse(null);

        if (name == null) {
          String originColumnName = field.getOriginColumnName().orElse(null);
          StringBuilder stringBuilder = new StringBuilder(COLUMN_NAME_PREFIX).append(columnNumber);
          // process of expr with Column, we record originColumnName in Field when analyze
          if (originColumnName != null) {
            stringBuilder.append('_').append(originColumnName);
          }
          name = stringBuilder.toString();
        }

        names.add(name);
        int fieldIndex = outputDescriptor.indexOf(field);
        Symbol symbol = plan.getSymbol(fieldIndex);
        outputs.add(symbol);

        columnHeaders.add(new ColumnHeader(name, getTSDataType(field.getType())));

        columnNumber++;
      }
    }

    OutputNode outputNode =
        new OutputNode(
            queryContext.getQueryId().genPlanNodeId(),
            plan.getRoot(),
            names.build(),
            outputs.build());

    DatasetHeader respDatasetHeader = new DatasetHeader(columnHeaders, true);
    analysis.setRespDatasetHeader(respDatasetHeader);

    return outputNode;
  }

  private RelationPlan createRelationPlan(Analysis analysis, WrappedStatement statement) {
    return getRelationPlanner(analysis).process(statement, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, LoadTsFile loadTsFile) {
    return getRelationPlanner(analysis).process(loadTsFile, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, PipeEnriched pipeEnriched) {
    return getRelationPlanner(analysis).process(pipeEnriched, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, Query query) {
    return getRelationPlanner(analysis).process(query, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, Table table) {
    return getRelationPlanner(analysis).process(table, null);
  }

  private RelationPlan createRelationPlan(Analysis analysis, Delete statement) {
    return getRelationPlanner(analysis).process(statement, null);
  }

  private RelationPlanner getRelationPlanner(Analysis analysis) {
    return new RelationPlanner(
        analysis, symbolAllocator, queryContext, Optional.empty(), sessionInfo, ImmutableMap.of());
  }

  private PlanNode planCreateOrUpdateDevice(
      final CreateOrUpdateDevice statement, final Analysis analysis) {
    final CreateOrUpdateTableDeviceNode node =
        new CreateOrUpdateTableDeviceNode(
            queryContext.getQueryId().genPlanNodeId(),
            statement.getDatabase(),
            statement.getTable(),
            statement.getDeviceIdList(),
            statement.getAttributeNameList(),
            statement.getAttributeValueList());

    analysis.setStatement(statement);
    final SchemaPartition partition =
        metadata.getOrCreateSchemaPartition(
            statement.getDatabase(),
            node.getPartitionKeyList(),
            queryContext.getSession().getUserName());
    analysis.setSchemaPartitionInfo(partition);

    return node;
  }

  private PlanNode planFetchDevice(final FetchDevice statement, final Analysis analysis) {
    final List<ColumnHeader> columnHeaderList =
        getDeviceColumnHeaderList(statement.getDatabase(), statement.getTableName(), null);

    analysis.setRespDatasetHeader(new DatasetHeader(columnHeaderList, true));

    final TableDeviceFetchNode fetchNode =
        new TableDeviceFetchNode(
            queryContext.getQueryId().genPlanNodeId(),
            statement.getDatabase(),
            statement.getTableName(),
            statement.getDeviceIdList(),
            statement.getPartitionKeyList(),
            columnHeaderList,
            null);

    final SchemaPartition schemaPartition =
        metadata.getSchemaPartition(statement.getDatabase(), statement.getPartitionKeyList());
    analysis.setSchemaPartitionInfo(schemaPartition);

    if (schemaPartition.isEmpty()) {
      analysis.setFinishQueryAfterAnalyze();
    }

    return fetchNode;
  }

  private PlanNode planShowDevice(final ShowDevice statement, final Analysis analysis) {
    planQueryDevice(statement, analysis);

    final QueryId queryId = queryContext.getQueryId();

    long pushDownLimit =
        Objects.nonNull(statement.getLimit())
            ? analysis.getLimit(statement.getLimit()).orElse(-1)
            : -1;
    if (pushDownLimit > -1 && Objects.nonNull(statement.getOffset())) {
      pushDownLimit += analysis.getOffset(statement.getOffset());
    }

    // Scan
    PlanNode currentNode =
        new TableDeviceQueryScanNode(
            queryId.genPlanNodeId(),
            statement.getDatabase(),
            statement.getTableName(),
            statement.getTagDeterminedFilterList(),
            null,
            statement.getColumnHeaderList(),
            null,
            Objects.isNull(statement.getTagFuzzyPredicate()) ? pushDownLimit : -1,
            statement.needAligned());

    // put the column type info into symbolAllocator to generate TypeProvider
    statement
        .getColumnHeaderList()
        .forEach(
            columnHeader ->
                symbolAllocator.newSymbol(
                    columnHeader.getColumnName(),
                    TypeFactory.getType(columnHeader.getColumnType())));

    // Filter
    if (Objects.nonNull(statement.getTagFuzzyPredicate())) {
      currentNode =
          new FilterNode(queryId.genPlanNodeId(), currentNode, statement.getTagFuzzyPredicate());
    }

    // Limit
    if (pushDownLimit > -1) {
      currentNode =
          new LimitNode(queryId.genPlanNodeId(), currentNode, pushDownLimit, Optional.empty());
    }

    // Offset
    return Objects.nonNull(statement.getOffset())
        ? new OffsetNode(
            queryId.genPlanNodeId(), currentNode, analysis.getOffset(statement.getOffset()))
        : currentNode;
  }

  private PlanNode planCountDevice(final CountDevice statement, final Analysis analysis) {
    planQueryDevice(statement, analysis);

    final TableDeviceQueryCountNode node =
        new TableDeviceQueryCountNode(
            queryContext.getQueryId().genPlanNodeId(),
            statement.getDatabase(),
            statement.getTableName(),
            statement.getTagDeterminedFilterList(),
            statement.getTagFuzzyPredicate(),
            statement.getColumnHeaderList());

    // put the column type info into symbolAllocator to generate TypeProvider
    statement
        .getColumnHeaderList()
        .forEach(
            columnHeader ->
                symbolAllocator.newSymbol(
                    columnHeader.getColumnName(),
                    TypeFactory.getType(columnHeader.getColumnType())));
    symbolAllocator.newSymbol(COUNT_DEVICE_HEADER_STRING, LongType.INT64);
    final CountSchemaMergeNode countMergeNode =
        new CountSchemaMergeNode(queryContext.getQueryId().genPlanNodeId());
    countMergeNode.addChild(node);
    return countMergeNode;
  }

  private void planQueryDevice(
      final AbstractQueryDeviceWithCache statement, final Analysis analysis) {
    planTraverseDevice(statement, analysis);

    if (!analysis.isFailed()) {
      analysis.setRespDatasetHeader(statement.getDataSetHeader());
    }
  }

  private PlanNode planUpdate(final Update statement, final Analysis analysis) {
    planTraverseDevice(statement, analysis);

    return new TableDeviceAttributeUpdateNode(
        queryContext.getQueryId().genPlanNodeId(),
        statement.getDatabase(),
        statement.getTableName(),
        statement.getTagDeterminedFilterList(),
        statement.getTagFuzzyPredicate(),
        statement.getColumnHeaderList(),
        null,
        statement.getAssignments(),
        queryContext.getSession());
  }

  private void planTraverseDevice(final AbstractTraverseDevice statement, final Analysis analysis) {
    final String database = statement.getDatabase();

    final TsTable table =
        DataNodeTableCache.getInstance().getTable(database, statement.getTableName());

    if (TreeViewSchema.isTreeViewTable(table)) {
      final PathPatternTree tree = new PathPatternTree();
      tree.appendPathPattern(TreeViewSchema.getPrefixPattern(table));
      tree.constructTree();

      analysis.setSchemaPartitionInfo(
          ClusterPartitionFetcher.getInstance().getSchemaPartition(tree));

      if (analysis.getSchemaPartitionInfo().getSchemaPartitionMap().size() > 1) {
        throw new SemanticException("Tree device view with multiple databases is unsupported yet.");
      }
    } else {
      analysis.setSchemaPartitionInfo(
          statement.isIdDetermined()
              ? metadata.getSchemaPartition(database, statement.getPartitionKeyList())
              : metadata.getSchemaPartition(database));
    }

    if (analysis.getSchemaPartitionInfo().isEmpty()) {
      analysis.setFinishQueryAfterAnalyze();
    }
  }

  private RelationPlan planExplainAnalyze(final ExplainAnalyze statement, final Analysis analysis) {
    RelationPlan originalQueryPlan =
        createRelationPlan(analysis, (Query) (statement.getStatement()));
    Symbol symbol =
        symbolAllocator.newSymbol(ColumnHeaderConstant.EXPLAIN_ANALYZE, StringType.getInstance());

    // recording permittedOutputs of ExplainAnalyzeNode's child
    RelationType outputDescriptor = analysis.getOutputDescriptor(statement.getStatement());
    ImmutableList.Builder<Symbol> childPermittedOutputs = ImmutableList.builder();
    for (Field field : outputDescriptor.getVisibleFields()) {
      int fieldIndex = outputDescriptor.indexOf(field);
      Symbol columnSymbol = originalQueryPlan.getSymbol(fieldIndex);
      childPermittedOutputs.add(columnSymbol);
    }

    PlanNode newRoot =
        new ExplainAnalyzeNode(
            queryContext.getQueryId().genPlanNodeId(),
            originalQueryPlan.getRoot(),
            statement.isVerbose(),
            queryContext.getLocalQueryId(),
            queryContext.getTimeOut(),
            symbol,
            childPermittedOutputs.build());
    return new RelationPlan(
        newRoot,
        originalQueryPlan.getScope(),
        originalQueryPlan.getFieldMappings(),
        Optional.empty());
  }

  private enum Stage {
    CREATED,
    OPTIMIZED,
    OPTIMIZED_AND_VALIDATED
  }
}
