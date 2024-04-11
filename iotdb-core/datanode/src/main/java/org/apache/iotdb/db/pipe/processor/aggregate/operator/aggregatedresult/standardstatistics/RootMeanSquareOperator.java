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

package org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.standardstatistics;

import org.apache.iotdb.db.pipe.processor.aggregate.operator.aggregatedresult.AggregatedResultOperator;
import org.apache.iotdb.db.pipe.processor.aggregate.operator.intermediateresult.CustomizedReadableIntermediateResults;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RootMeanSquareOperator implements AggregatedResultOperator {
  @Override
  public String getName() {
    return "rms";
  }

  @Override
  public void configureSystemParameters(Map<String, String> systemParams) {
    // Do nothing
  }

  @Override
  public Set<String> getDeclaredIntermediateValueNames() {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList("sum_x2", "count")));
  }

  @Override
  public Pair<TSDataType, Object> terminateWindow(
      TSDataType measurementDataType, CustomizedReadableIntermediateResults intermediateResults) {
    return new Pair<>(
        TSDataType.DOUBLE,
        Math.pow(
            intermediateResults.getDouble("sum_x2") / intermediateResults.getInt("count"), 0.5));
  }
}
