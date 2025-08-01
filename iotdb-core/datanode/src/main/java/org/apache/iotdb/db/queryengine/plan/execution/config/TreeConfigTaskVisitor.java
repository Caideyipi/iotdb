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

package org.apache.iotdb.db.queryengine.plan.execution.config;

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CountDatabaseTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CountTimeSlotListTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CreateContinuousQueryTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CreateFunctionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CreatePipePluginTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.CreateTriggerTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DatabaseSchemaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DeleteStorageGroupTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DeleteTimeSeriesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DropContinuousQueryTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DropFunctionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DropPipePluginTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.DropTriggerTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.GetRegionIdTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.GetSeriesSlotListTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.GetTimeSlotListTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.RemoveAINodeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.RemoveConfigNodeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.RemoveDataNodeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.SetTTLTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowAINodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterDetailsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterIdTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowClusterTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowConfigNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowContinuousQueriesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowDataNodesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowDatabaseTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowFunctionsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowPipePluginsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowTTLTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowTriggersTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ShowVariablesTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.UnSetTTLTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ai.CreateModelTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ai.CreateTrainingTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ai.DropModelTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.ai.ShowModelsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.ExtendRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.MigrateRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.ReconstructRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region.RemoveRegionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.AlterSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.CreateSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.DeactivateSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.DropSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.SetSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.ShowNodesInSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.ShowPathSetTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.ShowSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.template.UnsetSchemaTemplateTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.view.AlterLogicalViewTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.view.DeleteLogicalViewTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.view.RenameLogicalViewTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.SetSqlDialectTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowCurrentSqlDialectTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.session.ShowCurrentUserTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.AuthorizerTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.ClearCacheTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.FlushTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.KillQueryTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.LoadConfigurationTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.MergeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.SetConfigurationTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.SetSystemStatusTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.StartRepairDataTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.StopRepairDataTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.TestConnectionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.AlterPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.CreatePipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.DropPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.ShowPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.StartPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe.StopPipeTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota.SetSpaceQuotaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota.SetThrottleQuotaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota.ShowSpaceQuotaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota.ShowThrottleQuotaTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.CreateTopicTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.DropSubscriptionTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.DropTopicTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.ShowSubscriptionsTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription.ShowTopicsTask;
import org.apache.iotdb.db.queryengine.plan.statement.AuthorType;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CountTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DatabaseSchemaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DeleteTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropContinuousQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropFunctionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.DropTriggerStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetRegionIdStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetSeriesSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.GetTimeSlotListStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveAINodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveConfigNodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.RemoveDataNodeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.SetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterIdStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowClusterStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowConfigNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowContinuousQueriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDataNodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowDatabaseStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowFunctionsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowTriggersStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.ShowVariablesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.UnSetTTLStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.CreateModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.CreateTrainingStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.DropModelStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowAINodesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.model.ShowModelsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.AlterPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipePluginStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.DropPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipePluginsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.ShowPipesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StartPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.StopPipeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.ExtendRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.MigrateRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.ReconstructRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.RemoveRegionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.CreateTopicStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.DropSubscriptionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.DropTopicStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.ShowSubscriptionsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.ShowTopicsStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.AlterSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.CreateSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.DeactivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.DropSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.SetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowNodesInSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowPathSetTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.UnsetSchemaTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.AlterLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.DeleteLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.RenameLogicalViewStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.AuthorStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ClearCacheStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.FlushStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.KillQueryStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.LoadConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.MergeStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetConfigurationStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetSqlDialectStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.SetSystemStatusStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowCurrentSqlDialectStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowCurrentUserStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.StartRepairDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.StopRepairDataStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.TestConnectionStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetThrottleQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowSpaceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowThrottleQuotaStatement;
import org.apache.iotdb.db.utils.DataNodeAuthUtils;

import org.apache.tsfile.exception.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.executable.ExecutableManager.getUnTrustedUriErrorMsg;
import static org.apache.iotdb.commons.executable.ExecutableManager.isUriTrusted;
import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.checkAndEnrichSinkUserName;
import static org.apache.iotdb.db.queryengine.plan.execution.config.TableConfigTaskVisitor.checkAndEnrichSourceUserName;

public class TreeConfigTaskVisitor extends StatementVisitor<IConfigTask, MPPQueryContext> {

  @Override
  public IConfigTask visitNode(StatementNode node, MPPQueryContext context) {
    throw new UnsupportedOperationException(
        "Unsupported statement type: " + node.getClass().getName());
  }

  @Override
  public IConfigTask visitStatement(Statement statement, MPPQueryContext context) {
    throw new NotImplementedException("ConfigTask is not implemented for: " + statement);
  }

  @Override
  public IConfigTask visitSetDatabase(DatabaseSchemaStatement statement, MPPQueryContext context) {
    return new DatabaseSchemaTask(statement);
  }

  @Override
  public IConfigTask visitAlterDatabase(
      DatabaseSchemaStatement statement, MPPQueryContext context) {
    return new DatabaseSchemaTask(statement);
  }

  @Override
  public IConfigTask visitDeleteStorageGroup(
      DeleteDatabaseStatement statement, MPPQueryContext context) {
    return new DeleteStorageGroupTask(statement);
  }

  @Override
  public IConfigTask visitShowStorageGroup(
      ShowDatabaseStatement statement, MPPQueryContext context) {
    return new ShowDatabaseTask(statement);
  }

  @Override
  public IConfigTask visitCountStorageGroup(
      CountDatabaseStatement statement, MPPQueryContext context) {
    return new CountDatabaseTask(statement);
  }

  @Override
  public IConfigTask visitSetTTL(SetTTLStatement statement, MPPQueryContext context) {
    return new SetTTLTask(statement);
  }

  @Override
  public IConfigTask visitUnSetTTL(UnSetTTLStatement statement, MPPQueryContext context) {
    return new UnSetTTLTask(statement);
  }

  @Override
  public IConfigTask visitShowTTL(ShowTTLStatement showTTLStatement, MPPQueryContext context) {
    return new ShowTTLTask(showTTLStatement);
  }

  @Override
  public IConfigTask visitShowVariables(
      ShowVariablesStatement showVariablesStatement, MPPQueryContext context) {
    return new ShowVariablesTask();
  }

  @Override
  public IConfigTask visitShowCluster(
      ShowClusterStatement showClusterStatement, MPPQueryContext context) {
    if (showClusterStatement.isDetails()) {
      return new ShowClusterDetailsTask(showClusterStatement);
    } else {
      return new ShowClusterTask(showClusterStatement);
    }
  }

  @Override
  public IConfigTask visitShowClusterId(
      ShowClusterIdStatement showClusterIdStatement, MPPQueryContext context) {
    return new ShowClusterIdTask();
  }

  @Override
  public IConfigTask visitTestConnection(
      TestConnectionStatement testConnectionStatement, MPPQueryContext context) {
    return new TestConnectionTask(testConnectionStatement.needDetails());
  }

  @Override
  public IConfigTask visitAuthor(AuthorStatement statement, MPPQueryContext context) {
    if (statement.getAuthorType() == AuthorType.UPDATE_USER) {
      visitUpdateUser(statement);
    }
    return new AuthorizerTask(statement);
  }

  private void visitUpdateUser(AuthorStatement statement) {
    User user = AuthorityChecker.getAuthorityFetcher().getUser(statement.getUserName());
    if (user == null) {
      throw new SemanticException("User " + statement.getUserName() + " not found");
    }
    statement.setPassWord(user.getPassword());
    DataNodeAuthUtils.verifyPasswordReuse(statement.getUserName(), statement.getNewPassword());
  }

  @Override
  public IConfigTask visitMerge(MergeStatement mergeStatement, MPPQueryContext context) {
    return new MergeTask(mergeStatement);
  }

  @Override
  public IConfigTask visitFlush(FlushStatement flushStatement, MPPQueryContext context) {
    return new FlushTask(flushStatement);
  }

  @Override
  public IConfigTask visitClearCache(
      ClearCacheStatement clearCacheStatement, MPPQueryContext context) {
    return new ClearCacheTask(clearCacheStatement);
  }

  @Override
  public IConfigTask visitSetConfiguration(
      SetConfigurationStatement setConfigurationStatement, MPPQueryContext context) {
    return new SetConfigurationTask(setConfigurationStatement);
  }

  @Override
  public IConfigTask visitStartRepairData(
      StartRepairDataStatement startRepairDataStatement, MPPQueryContext context) {
    return new StartRepairDataTask(startRepairDataStatement);
  }

  @Override
  public IConfigTask visitStopRepairData(
      StopRepairDataStatement stopRepairDataStatement, MPPQueryContext context) {
    return new StopRepairDataTask(stopRepairDataStatement);
  }

  @Override
  public IConfigTask visitLoadConfiguration(
      LoadConfigurationStatement loadConfigurationStatement, MPPQueryContext context) {
    return new LoadConfigurationTask(loadConfigurationStatement);
  }

  @Override
  public IConfigTask visitSetSystemStatus(
      SetSystemStatusStatement setSystemStatusStatement, MPPQueryContext context) {
    return new SetSystemStatusTask(setSystemStatusStatement);
  }

  @Override
  public IConfigTask visitKillQuery(
      KillQueryStatement killQueryStatement, MPPQueryContext context) {
    return new KillQueryTask(killQueryStatement);
  }

  @Override
  public IConfigTask visitCreateFunction(
      CreateFunctionStatement createFunctionStatement, MPPQueryContext context) {
    if (createFunctionStatement.getUriString().map(ExecutableManager::isUriTrusted).orElse(true)) {
      // 1. user specified uri and that uri is trusted
      // 2. user doesn't specify uri
      return new CreateFunctionTask(createFunctionStatement);
    } else {
      // user specified uri and that uri is not trusted
      throw new SemanticException(
          getUnTrustedUriErrorMsg(createFunctionStatement.getUriString().get()));
    }
  }

  @Override
  public IConfigTask visitDropFunction(
      DropFunctionStatement dropFunctionStatement, MPPQueryContext context) {
    return new DropFunctionTask(Model.TREE, dropFunctionStatement.getUdfName());
  }

  @Override
  public IConfigTask visitShowFunctions(
      ShowFunctionsStatement showFunctionsStatement, MPPQueryContext context) {
    return new ShowFunctionsTask(Model.TREE);
  }

  @Override
  public IConfigTask visitCreateTrigger(
      CreateTriggerStatement createTriggerStatement, MPPQueryContext context) {
    if (!createTriggerStatement.isUsingURI()
        || (createTriggerStatement.getUriString() != null
            && isUriTrusted(createTriggerStatement.getUriString()))) {
      // 1. user specified uri and that uri is trusted
      // 2. user doesn't specify uri
      return new CreateTriggerTask(createTriggerStatement);
    } else {
      // user specified uri and that uri is not trusted
      throw new SemanticException(getUnTrustedUriErrorMsg(createTriggerStatement.getUriString()));
    }
  }

  @Override
  public IConfigTask visitDropTrigger(
      DropTriggerStatement dropTriggerStatement, MPPQueryContext context) {
    return new DropTriggerTask(dropTriggerStatement);
  }

  @Override
  public IConfigTask visitShowTriggers(
      ShowTriggersStatement showTriggersStatement, MPPQueryContext context) {
    return new ShowTriggersTask();
  }

  @Override
  public IConfigTask visitCreatePipePlugin(
      CreatePipePluginStatement createPipePluginStatement, MPPQueryContext context) {
    if (createPipePluginStatement.getUriString() != null
        && isUriTrusted(createPipePluginStatement.getUriString())) {
      // 1. user specified uri and that uri is trusted
      // 2. user doesn't specify uri
      return new CreatePipePluginTask(createPipePluginStatement);
    } else {
      // user specified uri and that uri is not trusted
      throw new SemanticException(
          getUnTrustedUriErrorMsg(createPipePluginStatement.getUriString()));
    }
  }

  @Override
  public IConfigTask visitDropPipePlugin(
      DropPipePluginStatement dropPipePluginStatement, MPPQueryContext context) {
    return new DropPipePluginTask(dropPipePluginStatement);
  }

  @Override
  public IConfigTask visitShowPipePlugins(
      ShowPipePluginsStatement showPipePluginStatement, MPPQueryContext context) {
    return new ShowPipePluginsTask(showPipePluginStatement);
  }

  @Override
  public IConfigTask visitShowRegion(
      ShowRegionStatement showRegionStatement, MPPQueryContext context) {
    return new ShowRegionTask(showRegionStatement, false);
  }

  @Override
  public IConfigTask visitCreateSchemaTemplate(
      CreateSchemaTemplateStatement createSchemaTemplateStatement, MPPQueryContext context) {
    return new CreateSchemaTemplateTask(createSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitShowNodesInSchemaTemplate(
      ShowNodesInSchemaTemplateStatement showNodesInSchemaTemplateStatement,
      MPPQueryContext context) {
    return new ShowNodesInSchemaTemplateTask(showNodesInSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitShowSchemaTemplate(
      ShowSchemaTemplateStatement showSchemaTemplateStatement, MPPQueryContext context) {
    return new ShowSchemaTemplateTask(showSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitSetSchemaTemplate(
      SetSchemaTemplateStatement setSchemaTemplateStatement, MPPQueryContext context) {
    return new SetSchemaTemplateTask(context.getQueryId().getId(), setSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitShowPathSetTemplate(
      ShowPathSetTemplateStatement showPathSetTemplateStatement, MPPQueryContext context) {
    return new ShowPathSetTemplateTask(showPathSetTemplateStatement);
  }

  @Override
  public IConfigTask visitDeactivateTemplate(
      DeactivateTemplateStatement deactivateTemplateStatement, MPPQueryContext context) {
    return new DeactivateSchemaTemplateTask(
        context.getQueryId().getId(), deactivateTemplateStatement);
  }

  @Override
  public IConfigTask visitUnsetSchemaTemplate(
      UnsetSchemaTemplateStatement unsetSchemaTemplateStatement, MPPQueryContext context) {
    return new UnsetSchemaTemplateTask(context.getQueryId().getId(), unsetSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitDropSchemaTemplate(
      DropSchemaTemplateStatement dropSchemaTemplateStatement, MPPQueryContext context) {
    return new DropSchemaTemplateTask(dropSchemaTemplateStatement);
  }

  @Override
  public IConfigTask visitAlterSchemaTemplate(
      AlterSchemaTemplateStatement alterSchemaTemplateStatement, MPPQueryContext context) {
    return new AlterSchemaTemplateTask(alterSchemaTemplateStatement, context.getQueryId().getId());
  }

  @Override
  public IConfigTask visitShowDataNodes(
      ShowDataNodesStatement showDataNodesStatement, MPPQueryContext context) {
    return new ShowDataNodesTask(showDataNodesStatement);
  }

  @Override
  public IConfigTask visitShowConfigNodes(
      ShowConfigNodesStatement showConfigNodesStatement, MPPQueryContext context) {
    return new ShowConfigNodesTask();
  }

  @Override
  public IConfigTask visitShowAINodes(
      ShowAINodesStatement showAINodesStatement, MPPQueryContext context) {
    return new ShowAINodesTask();
  }

  @Override
  public IConfigTask visitShowPipes(
      ShowPipesStatement showPipesStatement, MPPQueryContext context) {
    return new ShowPipeTask(showPipesStatement);
  }

  @Override
  public IConfigTask visitDropPipe(DropPipeStatement dropPipeStatement, MPPQueryContext context) {
    return new DropPipeTask(dropPipeStatement);
  }

  @Override
  public IConfigTask visitCreatePipe(
      final CreatePipeStatement createPipeStatement, final MPPQueryContext context) {
    for (final String ExtractorAttribute : createPipeStatement.getExtractorAttributes().keySet()) {
      if (ExtractorAttribute.startsWith(SystemConstant.SYSTEM_PREFIX_KEY)) {
        throw new SemanticException(
            String.format(
                "Failed to create pipe %s, setting %s is not allowed.",
                createPipeStatement.getPipeName(), ExtractorAttribute));
      }
    }

    // Inject tree model into the extractor attributes
    createPipeStatement
        .getExtractorAttributes()
        .put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
    checkAndEnrichSourceUserName(
        createPipeStatement.getPipeName(),
        createPipeStatement.getExtractorAttributes(),
        context.getSession().getUserName(),
        false);
    checkAndEnrichSinkUserName(
        createPipeStatement.getPipeName(),
        createPipeStatement.getConnectorAttributes(),
        context.getSession().getUserName(),
        false);

    return new CreatePipeTask(createPipeStatement);
  }

  @Override
  public IConfigTask visitAlterPipe(
      final AlterPipeStatement alterPipeStatement, final MPPQueryContext context) {

    for (final String extractorAttributeKey :
        alterPipeStatement.getExtractorAttributes().keySet()) {
      if (extractorAttributeKey.startsWith(SystemConstant.SYSTEM_PREFIX_KEY)) {
        throw new SemanticException(
            String.format(
                "Failed to alter pipe %s, modifying %s is not allowed.",
                alterPipeStatement.getPipeName(), extractorAttributeKey));
      }
    }

    final String userName = context.getSession().getUserName();
    alterPipeStatement.setUserName(userName);

    final String pipeName = alterPipeStatement.getPipeName();
    final Map<String, String> extractorAttributes = alterPipeStatement.getExtractorAttributes();

    // If the source is replaced, sql-dialect uses the current Alter Pipe sql-dialect. If it is
    // modified, the original sql-dialect is used.
    if (alterPipeStatement.isReplaceAllExtractorAttributes()) {
      extractorAttributes.put(
          SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);
      checkAndEnrichSourceUserName(pipeName, extractorAttributes, userName, true);
    }

    if (alterPipeStatement.isReplaceAllConnectorAttributes()) {
      checkAndEnrichSinkUserName(
          pipeName, alterPipeStatement.getConnectorAttributes(), userName, true);
    }

    return new AlterPipeTask(alterPipeStatement);
  }

  @Override
  public IConfigTask visitStartPipe(
      StartPipeStatement startPipeStatement, MPPQueryContext context) {
    return new StartPipeTask(startPipeStatement);
  }

  @Override
  public IConfigTask visitStopPipe(StopPipeStatement stopPipeStatement, MPPQueryContext context) {
    return new StopPipeTask(stopPipeStatement);
  }

  public IConfigTask visitCreateTopic(
      CreateTopicStatement createTopicStatement, MPPQueryContext context) {
    // Inject tree model into the topic attributes
    createTopicStatement
        .getTopicAttributes()
        .put(SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE);

    return new CreateTopicTask(createTopicStatement);
  }

  @Override
  public IConfigTask visitDropTopic(
      DropTopicStatement dropTopicStatement, MPPQueryContext context) {
    return new DropTopicTask(dropTopicStatement);
  }

  @Override
  public IConfigTask visitShowTopics(
      ShowTopicsStatement showTopicsStatement, MPPQueryContext context) {
    return new ShowTopicsTask(showTopicsStatement);
  }

  @Override
  public IConfigTask visitShowSubscriptions(
      ShowSubscriptionsStatement showSubscriptionsStatement, MPPQueryContext context) {
    return new ShowSubscriptionsTask(showSubscriptionsStatement);
  }

  @Override
  public IConfigTask visitDropSubscription(
      DropSubscriptionStatement dropSubscriptionStatement, MPPQueryContext context) {
    return new DropSubscriptionTask(dropSubscriptionStatement);
  }

  @Override
  public IConfigTask visitDeleteTimeSeries(
      DeleteTimeSeriesStatement deleteTimeSeriesStatement, MPPQueryContext context) {
    return new DeleteTimeSeriesTask(context.getQueryId().getId(), deleteTimeSeriesStatement);
  }

  @Override
  public IConfigTask visitDeleteLogicalView(
      DeleteLogicalViewStatement deleteLogicalViewStatement, MPPQueryContext context) {
    return new DeleteLogicalViewTask(context.getQueryId().getId(), deleteLogicalViewStatement);
  }

  @Override
  public IConfigTask visitRenameLogicalView(
      RenameLogicalViewStatement renameLogicalViewStatement, MPPQueryContext context) {
    return new RenameLogicalViewTask(context.getQueryId().getId(), renameLogicalViewStatement);
  }

  @Override
  public IConfigTask visitAlterLogicalView(
      AlterLogicalViewStatement alterLogicalViewStatement, MPPQueryContext context) {
    return new AlterLogicalViewTask(alterLogicalViewStatement, context);
  }

  @Override
  public IConfigTask visitGetRegionId(
      GetRegionIdStatement getRegionIdStatement, MPPQueryContext context) {
    return new GetRegionIdTask(getRegionIdStatement);
  }

  @Override
  public IConfigTask visitGetSeriesSlotList(
      GetSeriesSlotListStatement getSeriesSlotListStatement, MPPQueryContext context) {
    return new GetSeriesSlotListTask(getSeriesSlotListStatement);
  }

  @Override
  public IConfigTask visitGetTimeSlotList(
      GetTimeSlotListStatement getTimeSlotListStatement, MPPQueryContext context) {
    return new GetTimeSlotListTask(getTimeSlotListStatement);
  }

  @Override
  public IConfigTask visitCountTimeSlotList(
      CountTimeSlotListStatement countTimeSlotListStatement, MPPQueryContext context) {
    return new CountTimeSlotListTask(countTimeSlotListStatement);
  }

  @Override
  public IConfigTask visitMigrateRegion(
      MigrateRegionStatement migrateRegionStatement, MPPQueryContext context) {
    return new MigrateRegionTask(migrateRegionStatement);
  }

  @Override
  public IConfigTask visitReconstructRegion(
      ReconstructRegionStatement reconstructRegionStatement, MPPQueryContext context) {
    return new ReconstructRegionTask(reconstructRegionStatement);
  }

  @Override
  public IConfigTask visitExtendRegion(
      ExtendRegionStatement extendRegionStatement, MPPQueryContext context) {
    return new ExtendRegionTask(extendRegionStatement);
  }

  @Override
  public IConfigTask visitRemoveRegion(
      RemoveRegionStatement removeRegionStatement, MPPQueryContext context) {
    return new RemoveRegionTask(removeRegionStatement);
  }

  @Override
  public IConfigTask visitRemoveDataNode(
      RemoveDataNodeStatement removeDataNodeStatement, MPPQueryContext context) {
    return new RemoveDataNodeTask(removeDataNodeStatement);
  }

  @Override
  public IConfigTask visitRemoveConfigNode(
      RemoveConfigNodeStatement removeConfigNodeStatement, MPPQueryContext context) {
    return new RemoveConfigNodeTask(removeConfigNodeStatement);
  }

  @Override
  public IConfigTask visitRemoveAINode(
      RemoveAINodeStatement removeAINodeStatement, MPPQueryContext context) {
    return new RemoveAINodeTask(removeAINodeStatement);
  }

  @Override
  public IConfigTask visitCreateContinuousQuery(
      CreateContinuousQueryStatement createContinuousQueryStatement, MPPQueryContext context) {
    return new CreateContinuousQueryTask(createContinuousQueryStatement, context);
  }

  @Override
  public IConfigTask visitDropContinuousQuery(
      DropContinuousQueryStatement dropContinuousQueryStatement, MPPQueryContext context) {
    return new DropContinuousQueryTask(dropContinuousQueryStatement);
  }

  @Override
  public IConfigTask visitShowContinuousQueries(
      ShowContinuousQueriesStatement showContinuousQueriesStatement, MPPQueryContext context) {
    return new ShowContinuousQueriesTask();
  }

  @Override
  public IConfigTask visitSetSpaceQuota(
      SetSpaceQuotaStatement setSpaceQuotaStatement, MPPQueryContext context) {
    return new SetSpaceQuotaTask(setSpaceQuotaStatement);
  }

  @Override
  public IConfigTask visitShowSpaceQuota(
      ShowSpaceQuotaStatement showSpaceQuotaStatement, MPPQueryContext context) {
    return new ShowSpaceQuotaTask(showSpaceQuotaStatement);
  }

  @Override
  public IConfigTask visitSetThrottleQuota(
      SetThrottleQuotaStatement setThrottleQuotaStatement, MPPQueryContext context) {
    return new SetThrottleQuotaTask(setThrottleQuotaStatement);
  }

  @Override
  public IConfigTask visitShowThrottleQuota(
      ShowThrottleQuotaStatement showThrottleQuotaStatement, MPPQueryContext context) {
    return new ShowThrottleQuotaTask(showThrottleQuotaStatement);
  }

  @Override
  public IConfigTask visitSetSqlDialect(
      SetSqlDialectStatement setSqlDialectStatement, MPPQueryContext context) {
    return new SetSqlDialectTask(setSqlDialectStatement.getSqlDialect());
  }

  /** AI Model Management */
  @Override
  public IConfigTask visitCreateModel(
      CreateModelStatement createModelStatement, MPPQueryContext context) {
    String uri = createModelStatement.getUri();
    if (uri != null && isUriTrusted(uri)) {
      // user specified uri and that uri is trusted
      return new CreateModelTask(createModelStatement.getModelId(), uri);
    } else {
      // user specified uri and that uri is not trusted
      throw new SemanticException(getUnTrustedUriErrorMsg(uri));
    }
  }

  @Override
  public IConfigTask visitDropModel(
      DropModelStatement dropModelStatement, MPPQueryContext context) {
    return new DropModelTask(dropModelStatement.getModelId());
  }

  @Override
  public IConfigTask visitShowModels(
      ShowModelsStatement showModelsStatement, MPPQueryContext context) {
    return new ShowModelsTask(showModelsStatement.getModelId());
  }

  @Override
  public IConfigTask visitShowCurrentUser(ShowCurrentUserStatement node, MPPQueryContext context) {
    return new ShowCurrentUserTask(context.getSession().getUserName());
  }

  @Override
  public IConfigTask visitShowCurrentSqlDialect(
      ShowCurrentSqlDialectStatement node, MPPQueryContext context) {
    return new ShowCurrentSqlDialectTask(context.getSession().getSqlDialect().name());
  }

  @Override
  public IConfigTask visitCreateTraining(
      CreateTrainingStatement createTrainingStatement, MPPQueryContext context) {
    List<PartialPath> partialPathList = createTrainingStatement.getTargetPathPatterns();
    List<String> targetPathPatterns = new ArrayList<>();
    for (PartialPath partialPath : partialPathList) {
      targetPathPatterns.add(partialPath.getFullPath());
    }
    return new CreateTrainingTask(
        createTrainingStatement.getModelId(),
        createTrainingStatement.getParameters(),
        createTrainingStatement.getTargetTimeRanges(),
        createTrainingStatement.getExistingModelId(),
        targetPathPatterns);
  }
}
