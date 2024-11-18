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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.cache.TableDeviceSchemaCache;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateOrUpdateDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FetchDevice;
import org.apache.iotdb.db.queryengine.plan.relational.sql.parser.SqlParser;
import org.apache.iotdb.db.schemaengine.table.DataNodeTableCache;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableDeviceSchemaFetcher.convertIdValuesToDeviceID;

public class TableDeviceSchemaValidator {
  private final SqlParser relationSqlParser = new SqlParser();

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final Coordinator coordinator = Coordinator.getInstance();

  private final TableDeviceSchemaFetcher fetcher = TableDeviceSchemaFetcher.getInstance();

  private TableDeviceSchemaValidator() {
    // do nothing
  }

  private static class TableDeviceSchemaValidatorHolder {
    private static final TableDeviceSchemaValidator INSTANCE = new TableDeviceSchemaValidator();
  }

  public static TableDeviceSchemaValidator getInstance() {
    return TableDeviceSchemaValidatorHolder.INSTANCE;
  }

  public void validateDeviceSchema(
      final ITableDeviceSchemaValidation schemaValidation, final MPPQueryContext context) {
    // High-cost operations, shall only be called once
    final TsTable table =
        DataNodeTableCache.getInstance()
            .getTable(schemaValidation.getDatabase(), schemaValidation.getTableName());
    final List<Object[]> deviceIdList = schemaValidation.getDeviceIdList();
    // Replace to original names before the attributes access schema engine
    final int[] attributeIdList =
        schemaValidation.getAttributeColumnNameList().stream()
            .mapToInt(table::getAttributeId)
            .toArray();
    final List<Object[]> attributeValueList = schemaValidation.getAttributeValueList();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Validating device schema {}.{} and other {} devices",
          schemaValidation.getTableName(),
          Arrays.toString(deviceIdList.get(0)),
          deviceIdList.size() - 1);
    }
    ValidateResult validateResult =
        validateDeviceSchemaInCache(
            schemaValidation, deviceIdList, attributeIdList, attributeValueList);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("{} devices are missing", validateResult.missingDeviceIndexList.size());
    }

    if (!validateResult.missingDeviceIndexList.isEmpty()) {
      validateResult =
          fetchAndValidateDeviceSchema(
              schemaValidation,
              validateResult,
              context,
              deviceIdList,
              attributeIdList,
              attributeValueList);
    }

    if (!validateResult.missingDeviceIndexList.isEmpty()
        || !validateResult.attributeUpdateDeviceIndexList.isEmpty()) {
      autoCreateOrUpdateDeviceSchema(
          schemaValidation,
          validateResult,
          context,
          deviceIdList,
          attributeIdList,
          attributeValueList);
    }
  }

  private ValidateResult validateDeviceSchemaInCache(
      final ITableDeviceSchemaValidation schemaValidation,
      final List<Object[]> deviceIdList,
      final int[] attributeKeyList,
      final List<Object[]> attributeValueList) {
    final ValidateResult result = new ValidateResult();

    for (int i = 0, size = deviceIdList.size(); i < size; i++) {
      final Map<Integer, Binary> attributeMap =
          TableDeviceSchemaCache.getInstance()
              .getDeviceAttribute(
                  schemaValidation.getDatabase(),
                  convertIdValuesToDeviceID(
                      schemaValidation.getTableName(), (String[]) deviceIdList.get(i)));
      if (attributeMap == null) {
        result.missingDeviceIndexList.add(i);
      } else {
        for (int j = 0, attributeSize = attributeKeyList.length; j < attributeSize; j++) {
          if (!Objects.equals(
              attributeMap.get(attributeKeyList[j]), attributeValueList.get(i)[j])) {
            result.attributeUpdateDeviceIndexList.add(i);
            break;
          }
        }
      }
    }
    return result;
  }

  private ValidateResult fetchAndValidateDeviceSchema(
      final ITableDeviceSchemaValidation schemaValidation,
      final ValidateResult previousValidateResult,
      final MPPQueryContext context,
      final List<Object[]> deviceIdList,
      final int[] attributeKeyList,
      final List<Object[]> attributeValueList) {
    final Map<IDeviceID, Map<Integer, Binary>> fetchedDeviceSchema =
        fetcher.fetchMissingDeviceSchemaForDataInsertion(
            new FetchDevice(
                schemaValidation.getDatabase(),
                schemaValidation.getTableName(),
                previousValidateResult.missingDeviceIndexList.stream()
                    .map(deviceIdList::get)
                    .collect(Collectors.toList())),
            context);

    final ValidateResult result = new ValidateResult();
    for (final int index : previousValidateResult.missingDeviceIndexList) {
      final Map<Integer, Binary> attributeMap =
          fetchedDeviceSchema.get(
              convertIdValuesToDeviceID(
                  schemaValidation.getTableName(), (String[]) deviceIdList.get(index)));
      if (attributeMap == null) {
        result.missingDeviceIndexList.add(index);
      } else {
        constructAttributeUpdateDeviceIndexList(
            attributeKeyList, attributeValueList, result, index, attributeMap);
      }
    }

    result.attributeUpdateDeviceIndexList.addAll(
        previousValidateResult.attributeUpdateDeviceIndexList);

    return result;
  }

  private void constructAttributeUpdateDeviceIndexList(
      final int[] attributeKeyList,
      final List<Object[]> attributeValueList,
      final ValidateResult result,
      final int index,
      final Map<Integer, Binary> attributeMap) {
    final Object[] deviceAttributeValueList = attributeValueList.get(index);
    for (int j = 0, size = attributeKeyList.length; j < size; j++) {
      if (deviceAttributeValueList[j] != null) {
        final int key = attributeKeyList[j];
        final Binary value = attributeMap.get(key);

        if (!deviceAttributeValueList[j].equals(value)) {
          result.attributeUpdateDeviceIndexList.add(index);
          break;
        }
      }
    }
  }

  private void autoCreateOrUpdateDeviceSchema(
      final ITableDeviceSchemaValidation schemaValidation,
      final ValidateResult previousValidateResult,
      final MPPQueryContext context,
      final List<Object[]> inputDeviceIdList,
      final int[] attributeKeyList,
      final List<Object[]> intPutAttributeValueList) {
    final int size =
        previousValidateResult.missingDeviceIndexList.size()
            + previousValidateResult.attributeUpdateDeviceIndexList.size();
    final List<Object[]> deviceIdList = new ArrayList<>(size);
    final List<Object[]> attributeValueList = new ArrayList<>(size);

    previousValidateResult.missingDeviceIndexList.forEach(
        index -> {
          deviceIdList.add(inputDeviceIdList.get(index));
          attributeValueList.add(intPutAttributeValueList.get(index));
        });

    previousValidateResult.attributeUpdateDeviceIndexList.forEach(
        index -> {
          deviceIdList.add(inputDeviceIdList.get(index));
          attributeValueList.add(intPutAttributeValueList.get(index));
        });

    final ExecutionResult executionResult =
        coordinator.executeForTableModel(
            new CreateOrUpdateDevice(
                schemaValidation.getDatabase(),
                schemaValidation.getTableName(),
                deviceIdList,
                attributeKeyList,
                attributeValueList),
            relationSqlParser,
            SessionManager.getInstance().getCurrSession(),
            SessionManager.getInstance().requestQueryId(),
            SessionManager.getInstance()
                .getSessionInfo(SessionManager.getInstance().getCurrSession()),
            "Create device or update device attribute for insert",
            LocalExecutionPlanner.getInstance().metadata,
            context == null || context.getQueryType().equals(QueryType.WRITE)
                ? config.getQueryTimeoutThreshold()
                : context.getTimeOut());
    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      throw new RuntimeException(
          new IoTDBException(
              executionResult.status.getMessage(), executionResult.status.getCode()));
    }
  }

  private static class ValidateResult {
    final List<Integer> missingDeviceIndexList = new ArrayList<>();
    final List<Integer> attributeUpdateDeviceIndexList = new ArrayList<>();
  }
}
